"""Controller: REST API, job store and scheduler loop in one process (the only writer of the store)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import statistics
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.staticfiles import StaticFiles
from nats.errors import TimeoutError as NatsTimeoutError
from pydantic import ValidationError

from . import __version__, bus
from .backends import CloudBackend, CloudStatus, create_backend
from .config import Settings
from .models import Job, JobEvent, JobState, JobSubmit, WorkerInfo
from .policy import BudgetState, Offer, PolicyConfig, decide
from .scheduler import WorkerRegistry, estimate_wait_s, local_capacity
from .store import JobStore

log = logging.getLogger("burst.controller")

SUBMIT_FAILURE_COOLDOWN_S = 30.0
HISTORY_INTERVAL_S = 2.0
HISTORY_SAMPLES = 900   # 30 minutes


def start_of_day(now: float) -> float:
    """Midnight UTC of the day containing `now` (daily budgets reset then; unix time has no leap seconds)."""
    return now - now % 86400


class Controller:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = JobStore(settings.db_path)
        self.workers = WorkerRegistry(settings.worker_timeout_s)
        self.policy: PolicyConfig = settings.policy
        self.backends: dict[str, CloudBackend] = {
            name: create_backend(name, options) for name, options in settings.backends.items()
        }
        self.backend_cooldown: dict[str, float] = {}   # backend -> time until which it is skipped
        self.history: deque[dict] = deque(maxlen=HISTORY_SAMPLES)
        self.tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self.nc = await bus.connect(self.settings.nats_url, name="controller")
        self.js = self.nc.jetstream()
        await bus.ensure_streams(self.js)
        await self.nc.subscribe(bus.HEARTBEAT, cb=self._on_heartbeat)
        self.events = await self.js.pull_subscribe(bus.EVENTS, durable=bus.CONTROLLER_CONSUMER,
                                                   stream=bus.EVENTS_STREAM)
        self.tasks = [asyncio.create_task(self._event_loop()),
                      asyncio.create_task(self._every(self.settings.schedule_interval_s, self.schedule_once)),
                      asyncio.create_task(self._every(self.settings.cloud_poll_interval_s, self.poll_cloud_once)),
                      asyncio.create_task(self._every(HISTORY_INTERVAL_S, self._record_sample))]

    async def stop(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for backend in self.backends.values():
            with contextlib.suppress(Exception):
                await asyncio.to_thread(backend.close)
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

    async def schedule_once(self, now: float | None = None) -> int:
        """
        One scheduling pass: fill free local slots, then let the policy decide for every job that is
        still waiting whether to burst it. Returns the number of jobs dispatched.
        """
        now = now or time.time()
        queued = self.store.queued()
        capacity = local_capacity(self.workers, self.store.dispatched_not_started("local"), now)
        local, waiting = queued[:capacity], queued[capacity:]
        for job in local:
            # Nats-Msg-Id lets JetStream drop a duplicate if we crash between publish and mark_dispatched
            await self.js.publish(bus.dispatch_subject("local"), job.model_dump_json().encode(),
                                  headers={"Nats-Msg-Id": job.id})
            self.store.mark_dispatched(job.id, "local", decision="free local slot")
        bursted = await self._burst(waiting, now)
        self.workers.prune()
        return len(local) + bursted

    def budget_state(self, now: float) -> BudgetState:
        active = self.store.active_cloud_jobs()
        return BudgetState(
            active_cloud_jobs=len(active),
            spend_rate_per_hour=sum(j.price_per_hour or 0 for j in active),
            spent_today=self.store.cloud_cost_since(start_of_day(now)) + sum(j.cost_estimate or 0 for j in active),
        )

    async def _burst(self, waiting: list[Job], now: float) -> int:
        if not waiting:
            return 0
        if not self.backends:
            for job in waiting:
                self.store.set_decision(job.id, "waiting for a free local slot")
            return 0

        total_slots = self.workers.total_slots(now)
        runtimes = self.store.recent_runtimes()
        typical_runtime = statistics.median(runtimes) if runtimes else self.policy.default_runtime_s
        budget = self.budget_state(now)
        per_backend = self.store.active_by_backend()

        dispatched = 0
        position = 0   # position among the jobs that stay in the local queue
        for job in waiting:
            predicted = estimate_wait_s(position, total_slots, 0, runtimes, self.policy.default_runtime_s)
            offers = [
                Offer(b.name, b.price_per_hour(job), b.startup_s, b.max_jobs - per_backend.get(b.name, 0))
                for b in self.backends.values() if self.backend_cooldown.get(b.name, 0) <= now
            ]
            decision = decide(job, now, predicted, job.est_runtime_s or typical_runtime, offers, budget, self.policy)
            if decision.backend is None:
                self.store.set_decision(job.id, decision.reason)
                position += 1
                continue

            backend = self.backends[decision.backend]
            try:
                external_id = await asyncio.to_thread(backend.submit, job)
            except Exception as exc:
                log.exception("submitting %s to %s failed", job.id, backend.name)
                self.backend_cooldown[backend.name] = now + SUBMIT_FAILURE_COOLDOWN_S
                self.store.set_decision(job.id, f"submit to {backend.name} failed ({type(exc).__name__}); "
                                                f"skipping it for {SUBMIT_FAILURE_COOLDOWN_S:.0f}s")
                position += 1
                continue

            self.store.mark_dispatched(job.id, backend.name, external_id=external_id,
                                       price_per_hour=decision.price_per_hour,
                                       cost_estimate=decision.cost_estimate, decision=decision.reason)
            log.info("burst %s to %s (%s): %s", job.id, backend.name, external_id, decision.reason)
            budget.active_cloud_jobs += 1
            budget.spend_rate_per_hour += decision.price_per_hour
            budget.spent_today += decision.cost_estimate
            per_backend[backend.name] = per_backend.get(backend.name, 0) + 1
            dispatched += 1
        return dispatched

    # -- cloud jobs ------------------------------------------------------------------------------------

    def _apply_cloud_status(self, job: Job, st: CloudStatus, now: float) -> None:
        if st.state in ("running", "succeeded", "failed") and job.state == JobState.DISPATCHED:
            self.store.apply_event(JobEvent(job_id=job.id, kind="started", worker=job.backend,
                                            ts=st.started_at or now))
        if st.done:
            exit_code = st.exit_code if st.exit_code is not None else (0 if st.state == "succeeded" else None)
            finished = st.finished_at or now
            self.store.apply_event(JobEvent(job_id=job.id, kind="finished", worker=job.backend, ts=finished,
                                            exit_code=exit_code, error=st.error, output_tail=st.output_tail))
            self._charge(job, finished)

    def _charge(self, job: Job, end: float) -> None:
        """Actual cost: the backend's price for the time from submission to the end."""
        if job.price_per_hour is not None and job.dispatched_at is not None:
            self.store.set_cost(job.id, job.price_per_hour * max(0.0, end - job.dispatched_at) / 3600)

    async def poll_cloud_once(self, now: float | None = None) -> None:
        by_backend: dict[str, list[Job]] = {}
        for job in self.store.active_cloud_jobs():
            by_backend.setdefault(job.backend, []).append(job)
        for name, jobs in by_backend.items():
            backend = self.backends.get(name)
            if backend is None:
                continue
            try:
                statuses = await asyncio.to_thread(backend.status, [j.external_id for j in jobs])
            except Exception:
                log.exception("polling %s failed", name)
                continue
            now = now or time.time()
            for job in jobs:
                if job.external_id in statuses:
                    self._apply_cloud_status(job, statuses[job.external_id], now)

    async def _every(self, interval_s: float, step) -> None:
        """Run `step` forever, `interval_s` apart; a failing step is logged and retried next time."""
        while True:
            try:
                await step()
            except Exception:
                log.exception("%s failed", step.__name__)
            await asyncio.sleep(interval_s)

    # -- history for the dashboard ---------------------------------------------------------------------

    def sample(self, now: float | None = None) -> dict:
        now = now or time.time()
        active = self.store.active_by_backend()
        cloud = self.budget_state(now)
        return {
            "ts": now,
            "queued": self.store.count_by_state()["queued"],
            "local": active.get("local", 0),
            "cloud": sum(n for backend, n in active.items() if backend != "local"),
            "spend_rate_per_hour": cloud.spend_rate_per_hour,
        }

    async def _record_sample(self) -> None:
        self.history.append(self.sample())

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
        backend = self.backends.get(job.backend or "")
        if backend is not None and job.external_id:
            try:
                await asyncio.to_thread(backend.cancel, job.external_id)
            except Exception:
                log.exception("cancelling %s on %s failed", job.id, backend.name)
            self._charge(job, time.time())
        elif job.state != JobState.QUEUED:
            await self.nc.publish(bus.CANCEL, job_id.encode())
        return self.store.get(job_id)

    def update_policy(self, update: dict) -> PolicyConfig:
        try:
            self.policy = PolicyConfig(**{**self.policy.model_dump(), **update})
        except ValidationError as exc:
            raise HTTPException(422, exc.errors(include_url=False, include_context=False)) from exc
        return self.policy

    def stats(self, now: float | None = None) -> dict:
        now = now or time.time()
        queued = self.store.queued()
        waits = sorted(self.store.recent_waits(now=now))
        total_slots, free_slots = self.workers.total_slots(now), self.workers.free_slots(now)
        return {
            "counts": self.store.count_by_state(),
            "queue_depth": len(queued),
            "oldest_wait_s": max((j.wait_s(now) for j in queued), default=0.0),
            "wait_p50_s": statistics.median(waits) if waits else None,
            "wait_p95_s": waits[min(len(waits) - 1, int(0.95 * len(waits)))] if waits else None,
            "workers": len(self.workers.alive(now)),
            "slots_total": total_slots,
            "slots_free": free_slots,
            # expected wait for a job submitted now, if it stays local
            "estimated_wait_s": estimate_wait_s(len(queued), total_slots, free_slots,
                                                self.store.recent_runtimes(), self.policy.default_runtime_s),
            "cloud": self.cloud_stats(now),
        }

    def cloud_stats(self, now: float) -> dict:
        budget = self.budget_state(now)
        return {
            "active": budget.active_cloud_jobs,
            "active_by_backend": {b: n for b, n in self.store.active_by_backend().items() if b != "local"},
            "spend_rate_per_hour": budget.spend_rate_per_hour,
            "spent_today": budget.spent_today,
            "daily_budget": self.policy.daily_budget,
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

    @app.get("/stats/history", dependencies=auth)
    async def get_history(since: float = 0) -> list[dict]:
        """Samples every 2 s for the last 30 minutes (newer than `since`, a unix time)."""
        return [h for h in controller.history if h["ts"] > since]

    @app.get("/policy", dependencies=auth)
    async def get_policy() -> dict:
        return controller.policy.model_dump()

    @app.put("/policy", dependencies=auth)
    async def put_policy(update: dict) -> dict:
        return controller.update_policy(update).model_dump()

    @app.get("/backends", dependencies=auth)
    async def list_backends() -> list[dict]:
        active = controller.store.active_by_backend()
        return [{**b.describe(), "active": active.get(b.name, 0),
                 "cooling_down": controller.backend_cooldown.get(b.name, 0) > time.time()}
                for b in controller.backends.values()]

    # the dashboard (built with `npm run build` in dashboard/), mounted last so API routes win
    if Path(settings.dashboard_dir, "index.html").is_file():
        app.mount("/", StaticFiles(directory=settings.dashboard_dir, html=True), name="dashboard")

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
