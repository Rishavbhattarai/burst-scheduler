"""Data types shared by the controller, the workers and the API."""

from __future__ import annotations

import enum
import time
import uuid

from pydantic import BaseModel, Field


class JobState(enum.StrEnum):
    QUEUED = "queued"          # waiting in the controller's queue
    DISPATCHED = "dispatched"  # sent to a backend, not started yet
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in (JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED)


class JobSubmit(BaseModel):
    """What a client sends to POST /jobs."""

    name: str = Field(default="job", max_length=200)
    command: list[str] = Field(min_length=1, description="argv to run, e.g. ['python', '-c', 'print(1)']")
    image: str = Field(default="python:3.12-slim", description="container image used by cloud backends")
    cpu: float = Field(default=1.0, gt=0, le=64)
    memory_mb: int = Field(default=512, gt=0, le=262_144)
    est_runtime_s: float | None = Field(default=None, gt=0, description="user estimate, used for wait predictions")
    priority: int = Field(default=0, ge=-100, le=100, description="higher runs first")
    deadline_s: float | None = Field(default=None, gt=0, description="should finish within this many seconds")
    timeout_s: float = Field(default=3600, gt=0, le=7 * 24 * 3600)


class Job(JobSubmit):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    state: JobState = JobState.QUEUED
    backend: str | None = None
    worker: str | None = None
    submitted_at: float = Field(default_factory=time.time)
    dispatched_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    exit_code: int | None = None
    error: str | None = None
    output_tail: str | None = None
    attempts: int = 0

    def wait_s(self, now: float | None = None) -> float:
        """Time spent waiting before starting (so far, if it has not started)."""
        end = self.started_at if self.started_at is not None else (now or time.time())
        return max(0.0, end - self.submitted_at)


class WorkerInfo(BaseModel):
    """Heartbeat a local worker publishes every few seconds."""

    id: str
    slots: int
    running: list[str] = Field(default_factory=list)
    hostname: str = ""
    last_seen: float = Field(default_factory=time.time)

    @property
    def free(self) -> int:
        return max(0, self.slots - len(self.running))


class JobEvent(BaseModel):
    """Status update a worker publishes for a job."""

    job_id: str
    kind: str  # "started" | "finished"
    worker: str
    ts: float = Field(default_factory=time.time)
    exit_code: int | None = None
    error: str | None = None
    output_tail: str | None = None
