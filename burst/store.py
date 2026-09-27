"""SQLite job store. Only the controller writes to it."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterable

from .models import Job, JobEvent, JobState

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    command       TEXT NOT NULL,      -- JSON list
    image         TEXT NOT NULL,
    cpu           REAL NOT NULL,
    memory_mb     INTEGER NOT NULL,
    est_runtime_s REAL,
    priority      INTEGER NOT NULL,
    deadline_s    REAL,
    timeout_s     REAL NOT NULL,
    state         TEXT NOT NULL,
    backend       TEXT,
    worker        TEXT,
    submitted_at  REAL NOT NULL,
    dispatched_at REAL,
    started_at    REAL,
    finished_at   REAL,
    exit_code     INTEGER,
    error         TEXT,
    output_tail   TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0,
    external_id   TEXT,
    price_per_hour REAL,
    cost_estimate REAL,
    cost          REAL,
    decision      TEXT
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state, priority DESC, submitted_at);
"""

# columns added after the first release: (name, SQL type)
MIGRATIONS = [("external_id", "TEXT"), ("price_per_hour", "REAL"), ("cost_estimate", "REAL"), ("cost", "REAL"),
              ("decision", "TEXT")]


class JobStore:
    def __init__(self, path: str = ":memory:"):
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        if path != ":memory:":
            self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        existing = {row["name"] for row in self.db.execute("PRAGMA table_info(jobs)")}
        for column, sql_type in MIGRATIONS:
            if column not in existing:
                self.db.execute(f"ALTER TABLE jobs ADD COLUMN {column} {sql_type}")

    # -- reading ---------------------------------------------------------------------------------------

    @staticmethod
    def _to_job(row: sqlite3.Row) -> Job:
        data = dict(row)
        data["command"] = json.loads(data["command"])
        return Job(**data)

    def get(self, job_id: str) -> Job | None:
        row = self.db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._to_job(row) if row else None

    def list(self, state: JobState | None = None, limit: int = 100) -> list[Job]:
        rows = self.db.execute("SELECT * FROM jobs WHERE ?1 IS NULL OR state = ?1 ORDER BY submitted_at DESC LIMIT ?2",
                               (state and state.value, limit))
        return [self._to_job(r) for r in rows]

    def queued(self) -> list[Job]:
        """Queued jobs in the order they should run: priority, then first come first served."""
        rows = self.db.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY priority DESC, submitted_at", (JobState.QUEUED.value,)
        )
        return [self._to_job(r) for r in rows]

    def count_by_state(self) -> dict[str, int]:
        counts = {s.value: 0 for s in JobState}
        for row in self.db.execute("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state"):
            counts[row["state"]] = row["n"]
        return counts

    def active_by_backend(self) -> dict[str, int]:
        """Dispatched or running jobs per backend."""
        rows = self.db.execute(
            "SELECT backend, COUNT(*) FROM jobs WHERE state IN (?, ?) GROUP BY backend",
            (JobState.DISPATCHED.value, JobState.RUNNING.value),
        )
        return {r[0]: r[1] for r in rows}

    def dispatched_not_started(self, backend: str) -> int:
        row = self.db.execute(
            "SELECT COUNT(*) FROM jobs WHERE state = ? AND backend = ?", (JobState.DISPATCHED.value, backend)
        ).fetchone()
        return row[0]

    def recent_waits(self, since_s: float = 300, now: float | None = None) -> list[float]:
        """Wait times (start - submit) of jobs that started in the last since_s seconds."""
        now = now or time.time()
        rows = self.db.execute(
            "SELECT started_at - submitted_at FROM jobs WHERE started_at >= ?", (now - since_s,)
        )
        return [r[0] for r in rows]

    def recent_runtimes(self, limit: int = 50) -> list[float]:
        rows = self.db.execute(
            "SELECT finished_at - started_at FROM jobs WHERE state = ? AND started_at IS NOT NULL "
            "ORDER BY finished_at DESC LIMIT ?",
            (JobState.SUCCEEDED.value, limit),
        )
        return [r[0] for r in rows]

    # -- writing ---------------------------------------------------------------------------------------

    def add(self, job: Job) -> Job:
        data = job.model_dump(mode="json")
        data["command"] = json.dumps(data["command"])
        self.db.execute(f"INSERT INTO jobs ({', '.join(data)}) VALUES ({', '.join(':' + k for k in data)})", data)
        return job

    def _update(self, job_id: str, where_states: Iterable[JobState], **fields) -> bool:
        """Update fields if the job is in one of where_states. Returns whether a row changed."""
        states = [s.value for s in where_states]
        assignments = ", ".join(f"{k} = ?" for k in fields)
        values = [v.value if isinstance(v, JobState) else v for v in fields.values()]
        cur = self.db.execute(
            f"UPDATE jobs SET {assignments} WHERE id = ? AND state IN ({', '.join('?' for _ in states)})",
            [*values, job_id, *states],
        )
        return cur.rowcount == 1

    def mark_dispatched(self, job_id: str, backend: str, now: float | None = None, **cloud) -> bool:
        """Queued -> dispatched. `cloud` can set external_id, price_per_hour, cost_estimate, decision."""
        return self._update(
            job_id, [JobState.QUEUED],
            state=JobState.DISPATCHED, backend=backend, dispatched_at=now or time.time(), **cloud,
        )

    def set_decision(self, job_id: str, decision: str) -> None:
        """Record why a queued job is still waiting (only written when it changes)."""
        self.db.execute("UPDATE jobs SET decision = ? WHERE id = ? AND state = ? AND decision IS NOT ?",
                        (decision, job_id, JobState.QUEUED.value, decision))

    def set_cost(self, job_id: str, cost: float) -> None:
        self.db.execute("UPDATE jobs SET cost = ? WHERE id = ?", (cost, job_id))

    def active_cloud_jobs(self) -> list[Job]:
        """Dispatched or running jobs on a backend other than the local workers."""
        rows = self.db.execute(
            "SELECT * FROM jobs WHERE backend IS NOT NULL AND backend != 'local' AND state IN (?, ?)",
            (JobState.DISPATCHED.value, JobState.RUNNING.value),
        )
        return [self._to_job(r) for r in rows]

    def cloud_cost_since(self, since: float) -> float:
        """Actual cost of cloud jobs that finished since `since`."""
        row = self.db.execute(
            "SELECT COALESCE(SUM(cost), 0) FROM jobs WHERE backend != 'local' AND finished_at >= ?", (since,)
        ).fetchone()
        return row[0]

    def cancel(self, job_id: str, now: float | None = None) -> bool:
        return self._update(
            job_id, [JobState.QUEUED, JobState.DISPATCHED, JobState.RUNNING],
            state=JobState.CANCELLED, finished_at=now or time.time(),
        )

    def apply_event(self, event: JobEvent) -> bool:
        """
        Apply a worker event. Events can arrive twice (at-least-once delivery) or after a
        cancellation, so each transition only happens from the states it is valid in.
        """
        if event.kind == "started":
            job = self.get(event.job_id)
            if job is None:
                return False
            if job.state == JobState.RUNNING and job.worker != event.worker:
                # redelivered after its worker disappeared: another attempt (keep the first start
                # time, so wait statistics measure the time until the job first started)
                return self._update(event.job_id, [JobState.RUNNING], worker=event.worker,
                                    attempts=job.attempts + 1)
            return self._update(
                event.job_id, [JobState.QUEUED, JobState.DISPATCHED],
                state=JobState.RUNNING, worker=event.worker, started_at=event.ts, attempts=job.attempts + 1,
            )
        if event.kind == "finished":
            state = JobState.SUCCEEDED if event.exit_code == 0 else JobState.FAILED
            return self._update(
                event.job_id, [JobState.DISPATCHED, JobState.RUNNING],
                state=state, worker=event.worker, finished_at=event.ts, exit_code=event.exit_code,
                error=event.error, output_tail=event.output_tail,
            )
        raise ValueError(f"unknown event kind {event.kind!r}")
