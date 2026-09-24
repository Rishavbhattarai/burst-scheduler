"""Local worker: pulls jobs from NATS when it has a free slot and runs them as subprocesses."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import socket
from collections import OrderedDict

from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeoutError

from . import bus
from .models import Job, JobEvent, WorkerInfo

log = logging.getLogger("burst.worker")

OUTPUT_TAIL_BYTES = 4000
# a job whose worker stops acknowledging it for this long is redelivered to another worker
ACK_WAIT_S = float(os.environ.get("BURST_ACK_WAIT", "30"))


def kill_tree(proc: asyncio.subprocess.Process) -> None:
    """Kill a job and everything it started (jobs run in their own process group)."""
    if proc.returncode is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        with contextlib.suppress(ProcessLookupError):
            proc.kill()


class Worker:
    def __init__(self, nats_url: str, slots: int, worker_id: str, heartbeat_s: float = 2.0,
                 shutdown_grace_s: float = 10.0):
        self.nats_url = nats_url
        self.slots = slots
        self.id = worker_id
        self.heartbeat_s = heartbeat_s
        self.shutdown_grace_s = shutdown_grace_s
        self.running: dict[str, asyncio.subprocess.Process | None] = {}
        self.tasks: set[asyncio.Task] = set()
        self.cancelled: OrderedDict[str, None] = OrderedDict()  # recent cancel requests (bounded)
        self.stopping = asyncio.Event()

    # -- messaging -------------------------------------------------------------------------------------

    async def _publish_event(self, event: JobEvent) -> None:
        await self.js.publish(bus.EVENTS, event.model_dump_json().encode())

    async def _heartbeat(self) -> None:
        info = WorkerInfo(id=self.id, slots=self.slots, running=list(self.running), hostname=socket.gethostname())
        await self.nc.publish(bus.HEARTBEAT, info.model_dump_json().encode())

    async def _heartbeat_loop(self) -> None:
        while not self.stopping.is_set():
            with contextlib.suppress(Exception):
                await self._heartbeat()
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.stopping.wait(), self.heartbeat_s)

    async def _on_cancel(self, msg: Msg) -> None:
        job_id = msg.data.decode()
        self.cancelled[job_id] = None
        while len(self.cancelled) > 10_000:
            self.cancelled.popitem(last=False)
        proc = self.running.get(job_id)
        if proc is not None and proc.returncode is None:
            log.info("cancelling %s", job_id)
            kill_tree(proc)

    # -- running jobs ----------------------------------------------------------------------------------

    async def _keep_alive(self, msg: Msg) -> None:
        """Tell JetStream we are still working on msg, so it is not redelivered to another worker."""
        while True:
            await asyncio.sleep(ACK_WAIT_S / 3)
            with contextlib.suppress(Exception):
                await msg.in_progress()

    async def _run(self, msg: Msg) -> None:
        job = Job.model_validate_json(msg.data)
        if job.id in self.cancelled:
            await msg.ack()
            return

        self.running[job.id] = None
        keep_alive = asyncio.create_task(self._keep_alive(msg))
        await self._publish_event(JobEvent(job_id=job.id, kind="started", worker=self.id))
        await self._heartbeat()
        log.info("started %s (%s)", job.id, job.name)

        exit_code, error, tail = await self._execute(job)
        keep_alive.cancel()

        if self.stopping.is_set() and exit_code is None:
            # interrupted by shutdown: do not ack, JetStream redelivers the job to another worker
            self.running.pop(job.id, None)
            return

        await self._publish_event(JobEvent(job_id=job.id, kind="finished", worker=self.id,
                                           exit_code=exit_code, error=error, output_tail=tail))
        await msg.ack()
        self.running.pop(job.id, None)
        await self._heartbeat()
        log.info("finished %s exit=%s", job.id, exit_code)

    async def _execute(self, job: Job) -> tuple[int | None, str | None, str]:
        """Run the job's command. Returns (exit code, error message, last bytes of output)."""
        try:
            proc = await asyncio.create_subprocess_exec(
                *job.command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError as exc:
            return 127, f"could not start command: {exc}", ""
        self.running[job.id] = proc

        tail = bytearray()

        async def read_output() -> None:
            while chunk := await proc.stdout.read(65536):
                tail.extend(chunk)
                del tail[:-OUTPUT_TAIL_BYTES]

        reader = asyncio.create_task(read_output())
        error = None
        try:
            await asyncio.wait_for(proc.wait(), job.timeout_s)
        except TimeoutError:
            kill_tree(proc)
            await proc.wait()
            error = f"timed out after {job.timeout_s:g} s"
        # a background process left behind by the job can keep stdout open: do not wait forever
        try:
            await asyncio.wait_for(reader, 5)
        except TimeoutError:
            pass
        text = tail.decode(errors="replace")

        if job.id in self.cancelled:
            return proc.returncode, "cancelled", text
        if self.stopping.is_set() and proc.returncode is not None and proc.returncode < 0:
            return None, None, text  # killed because the worker is shutting down
        return proc.returncode, error, text

    # -- main loop -------------------------------------------------------------------------------------

    async def run(self) -> None:
        self.nc = await bus.connect(self.nats_url, name=f"worker-{self.id}")
        self.js = self.nc.jetstream()
        await bus.ensure_streams(self.js)
        sub = await self.js.pull_subscribe(
            bus.dispatch_subject("local"), durable=bus.LOCAL_CONSUMER, stream=bus.DISPATCH_STREAM,
            config=bus.local_consumer_config(ACK_WAIT_S),
        )
        await self.nc.subscribe(bus.CANCEL, cb=self._on_cancel)
        heartbeat = asyncio.create_task(self._heartbeat_loop())
        log.info("worker %s ready with %d slots", self.id, self.slots)

        while not self.stopping.is_set():
            free = self.slots - len(self.running)
            if free <= 0:
                await asyncio.sleep(0.1)
                continue
            try:
                msgs = await sub.fetch(free, timeout=1)
            except (TimeoutError, NatsTimeoutError):
                continue
            for msg in msgs:
                task = asyncio.create_task(self._run(msg))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)

        await self._shutdown()
        heartbeat.cancel()
        # a pull subscription never finishes draining (nats-py), so remove it first
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
        await self.nc.drain()

    async def _shutdown(self) -> None:
        if self.tasks:
            log.info("waiting up to %.0f s for %d running jobs", self.shutdown_grace_s, len(self.tasks))
            _, pending = await asyncio.wait(self.tasks, timeout=self.shutdown_grace_s)
            if pending:
                for proc in self.running.values():
                    if proc is not None:
                        kill_tree(proc)
                await asyncio.wait(pending, timeout=5)

    def stop(self) -> None:
        self.stopping.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="burst local worker")
    parser.add_argument("--nats-url", default=os.environ.get("BURST_NATS_URL", "nats://127.0.0.1:4222"))
    parser.add_argument("--slots", type=int, default=int(os.environ.get("BURST_WORKER_SLOTS", os.cpu_count() or 1)))
    parser.add_argument("--id", default=os.environ.get("BURST_WORKER_ID", f"{socket.gethostname()}-{os.getpid()}"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    worker = Worker(args.nats_url, args.slots, args.id)

    async def runner() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, worker.stop)
        await worker.run()

    asyncio.run(runner())


if __name__ == "__main__":
    main()
