from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..models import Job


@dataclass
class CloudStatus:
    state: str                     # "pending" | "running" | "succeeded" | "failed"
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    error: str | None = None
    output_tail: str | None = None

    @property
    def done(self) -> bool:
        return self.state in ("succeeded", "failed")


class CloudBackend(ABC):
    """
    A place to run jobs other than the local workers. Methods are blocking (they call cloud APIs);
    the controller runs them in a thread.
    """

    kind = "cloud"

    def __init__(self, name: str, options: dict):
        self.name = name
        self.startup_s = float(options.get("startup_s", 30))
        self.max_jobs = int(options.get("max_jobs", 10))
        # on-demand style: $ per vCPU-hour plus $ per GB-hour of memory
        self.cpu_hour = float(options.get("cpu_hour", 0.05))
        self.gb_hour = float(options.get("gb_hour", 0.005))

    def price_per_hour(self, job: Job) -> float:
        return job.cpu * self.cpu_hour + job.memory_mb / 1024 * self.gb_hour

    @abstractmethod
    def submit(self, job: Job) -> str:
        """Start the job; returns the backend's id for it."""

    @abstractmethod
    def status(self, external_ids: list[str]) -> dict[str, CloudStatus]:
        """Current status of the given jobs (ids the backend no longer knows are reported as failed)."""

    @abstractmethod
    def cancel(self, external_id: str) -> None:
        """Stop the job if it is still running."""

    def close(self) -> None:  # noqa: B027 - optional hook, a no-op by default
        """Called when the controller stops. Real cloud jobs keep running (polling resumes on restart)."""

    def describe(self) -> dict:
        return {"name": self.name, "kind": self.kind, "startup_s": self.startup_s, "max_jobs": self.max_jobs,
                "cpu_hour": self.cpu_hour, "gb_hour": self.gb_hour}
