"""Controller: REST API, job store and scheduler loop in one process (the only writer of the store)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import statistics
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from nats.errors import TimeoutError as NatsTimeoutError

from . import __version__, bus
from .config import Settings
from .models import Job, JobEvent, JobState, JobSubmit, WorkerInfo
from .scheduler import WorkerRegistry, estimate_wait_s, plan
from .store import JobStore

log = logging.getLogger("burst.controller")


class Controller:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = JobStore(settings.db_path)
        self.workers = WorkerRegistry(settings.worker_timeout_s)
        self.tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self.nc = await bus.connect(self.settings.nats_url, name="controller")
        self.js = self.nc.jetstream()
        await bus.ensure_streams(self.js)
        await self.nc.subscribe(bus.HEARTBEAT, cb=self._on_heartbeat)
        self.events = await self.js.pull_subscribe(bus.EVENTS, durable=bus.CONTROLLER_CONSUMER,
                                                   stream=bus.EVENTS_STREAM)
        self.tasks = [asyncio.create_task(self._event_loop()), asyncio.create_task(self._schedule_loop())]

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        # a pull subscription never finishes draining (nats-py), so remove it first
        with contextlib.suppress(Exception):
            await self.events.unsubscribe()
        with contextlib.suppress(Exception):
            await self.nc.drain()

    # -- incoming messages -----------------------------------------------------------------------------

    async def _on_heartbeat(self, msg) -> None:
        with contextlib.suppress(ValueError):
            self.workers.update(WorkerInfo.model_validate_json(msg.data))

    async def _event_loop(self) -> None:
        while True:
            try:
                msgs = await self.events.fetch(100, timeout=1)
            except (TimeoutError, NatsTimeoutError):
                continue
            except Exception:
                log.exception("fetching events failed")
                await asyncio.sleep(1)
                continue
            for msg in msgs:
                try:
                    self.store.apply_event(JobEvent.model_validate_json(msg.data))
                except ValueError:
                    log.warning("dropping malformed event %r", msg.data[:200])
                await msg.ack()

    # -- scheduling ------------------------------------------------------------------------------------

    async def schedule_once(self) -> int:
        """Dispatch what the scheduler decides. Returns the number of jobs dispatched."""
        decision = plan(self.store.queued(), self.workers, self.store.dispatched_not_started("local"))
        for placement in decision.placements:
            job = placement.job
            # Nats-Msg-Id lets JetStream drop a duplicate if we crash between publish and mark_dispatched
            await self.js.publish(bus.dispatch_subject(placement.backend), job.model_dump_json().encode(),
                                  headers={"Nats-Msg-Id": job.id})
            self.store.mark_dispatched(job.id, placement.backend)
        self.workers.prune()
        return len(decision.placements)

    async def _schedule_loop(self) -> None:
        while True:
            try:
                await self.schedule_once()
            except Exception:
                log.exception("scheduling failed")
            await asyncio.sleep(self.settings.schedule_interval_s)

    # -- API helpers -----------------------------------------------------------------------------------

    async def submit(self, spec: JobSubmit) -> Job:
        job = self.store.add(Job(**spec.model_dump()))
        log.info("queued %s (%s)", job.id, job.name)
        return job

    async def cancel(self, job_id: str) -> Job:
        job = self.store.get(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "no such job")
        if not self.store.cancel(job_id):
            raise HTTPException(status.HTTP_409_CONFLICT, f"job is already {job.state.value}")
        if job.state != JobState.QUEUED:
            await self.nc.publish(bus.CANCEL, job_id.encode())
        return self.store.get(job_id)

    def stats(self, now: float | None = None) -> dict:
        now = now or time.time()
        queued = self.store.queued()
        waits = sorted(self.store.recent_waits(now=now))
        alive = self.workers.alive(now)
        total_slots = sum(w.slots for w in alive)
        free_slots = sum(w.free for w in alive)
        return {
            "counts": self.store.count_by_state(),
            "queue_depth": len(queued),
            "oldest_wait_s": max((j.wait_s(now) for j in queued), default=0.0),
            "wait_p50_s": statistics.median(waits) if waits else None,
            "wait_p95_s": waits[min(len(waits) - 1, int(0.95 * len(waits)))] if waits else None,
            "workers": len(alive),
            "slots_total": total_slots,
            "slots_free": free_slots,
            # expected wait for a job submitted now, if it stays local
            "estimated_wait_s": estimate_wait_s(len(queued), total_slots, free_slots,
                                                self.store.recent_runtimes()),
        }


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    controller = Controller(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await controller.start()
        yield
        await controller.stop()

    app = FastAPI(title="burst scheduler", version=__version__, lifespan=lifespan)
    app.state.controller = controller

    def check_token(authorization: str | None = Header(default=None)) -> None:
        if settings.api_token and authorization != f"Bearer {settings.api_token}":
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing or wrong API token")

    auth = [Depends(check_token)]

    @app.get("/healthz")
    async def healthz():
        return {"ok": controller.nc.is_connected, "version": __version__}

    @app.post("/jobs", status_code=status.HTTP_201_CREATED, dependencies=auth)
    async def submit_job(spec: JobSubmit) -> Job:
        return await controller.submit(spec)

    @app.get("/jobs", dependencies=auth)
    async def list_jobs(state: JobState | None = None, limit: int = Query(100, ge=1, le=1000)) -> list[Job]:
        return controller.store.list(state, limit)

    @app.get("/jobs/{job_id}", dependencies=auth)
    async def get_job(job_id: str) -> Job:
        job = controller.store.get(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "no such job")
        return job

    @app.post("/jobs/{job_id}/cancel", dependencies=auth)
    async def cancel_job(job_id: str) -> Job:
        return await controller.cancel(job_id)

    @app.get("/workers", dependencies=auth)
    async def list_workers() -> list[dict]:
        now = time.time()
        return [{**w.model_dump(), "alive": now - w.last_seen <= settings.worker_timeout_s, "free": w.free}
                for w in controller.workers.all()]

    @app.get("/stats", dependencies=auth)
    async def get_stats() -> dict:
        return controller.stats()

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="burst controller (REST API + scheduler)")
    parser.add_argument("--host", default=os.environ.get("BURST_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("BURST_PORT", "8000")))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    uvicorn.run(create_app(), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
