"""A stand-in cloud for demos and tests: runs jobs on this machine after a startup delay and bills them.

It behaves like a remote backend (asynchronous start, polling, cancellation, a price) without needing
a cloud account. Results are labelled with the backend name, so it is always clear they are simulated.
"""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field

from ..models import Job
from .base import CloudBackend, CloudStatus

OUTPUT_TAIL_BYTES = 4000


@dataclass
class _SimJob:
    job: Job
    submitted_at: float
    output: tempfile._TemporaryFileWrapper = field(default_factory=lambda: tempfile.TemporaryFile())
    proc: subprocess.Popen | None = None
    started_at: float | None = None
    cancelled: bool = False
    error: str | None = None


class SimulatedBackend(CloudBackend):
    kind = "simulated"

    def __init__(self, name: str, options: dict):
        super().__init__(name, options)
        self._jobs: dict[str, _SimJob] = {}
        self._lock = threading.Lock()

    def submit(self, job: Job) -> str:
        external_id = f"sim-{uuid.uuid4().hex[:10]}"
        with self._lock:
            self._jobs[external_id] = _SimJob(job, time.time())
        return external_id

    def _advance(self, sim: _SimJob, now: float) -> None:
        """Start the process once the simulated startup time has passed."""
        if sim.proc is None and not sim.cancelled and sim.error is None and now >= sim.submitted_at + self.startup_s:
            try:
                sim.proc = subprocess.Popen(sim.job.command, stdout=sim.output, stderr=subprocess.STDOUT,
                                            start_new_session=True)
                sim.started_at = now
            except OSError as exc:
                sim.error = f"could not start command: {exc}"
        if sim.proc is not None and sim.proc.poll() is None and now - sim.started_at > sim.job.timeout_s:
            self._kill(sim)
            sim.error = f"timed out after {sim.job.timeout_s:g} s"

    @staticmethod
    def _kill(sim: _SimJob) -> None:
        if sim.proc is not None and sim.proc.poll() is None:
            try:
                os.killpg(sim.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            sim.proc.wait()

    def status(self, external_ids: list[str]) -> dict[str, CloudStatus]:
        now = time.time()
        result = {}
        with self._lock:
            for external_id in external_ids:
                sim = self._jobs.get(external_id)
                if sim is None:
                    result[external_id] = CloudStatus("failed", error="unknown to the simulated backend")
                    continue
                self._advance(sim, now)
                if sim.cancelled:
                    result[external_id] = CloudStatus("failed", sim.started_at, now, error="cancelled")
                elif sim.proc is None and sim.error:
                    result[external_id] = CloudStatus("failed", now, now, exit_code=127, error=sim.error)
                elif sim.proc is None:
                    result[external_id] = CloudStatus("pending")
                elif sim.proc.poll() is None:
                    result[external_id] = CloudStatus("running", sim.started_at)
                else:
                    sim.output.seek(0)
                    tail = sim.output.read()[-OUTPUT_TAIL_BYTES:].decode(errors="replace")
                    code = sim.proc.returncode
                    result[external_id] = CloudStatus("succeeded" if code == 0 else "failed", sim.started_at, now,
                                                      exit_code=code, error=sim.error, output_tail=tail)
                if result[external_id].done:
                    sim.output.close()
                    del self._jobs[external_id]
        return result

    def close(self) -> None:
        """The simulated jobs are child processes of the controller: stop them with it."""
        with self._lock:
            for sim in self._jobs.values():
                self._kill(sim)
                sim.output.close()
            self._jobs.clear()

    def cancel(self, external_id: str) -> None:
        with self._lock:
            sim = self._jobs.get(external_id)
            if sim is not None:
                sim.cancelled = True
                self._kill(sim)
