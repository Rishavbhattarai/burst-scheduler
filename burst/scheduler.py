"""Local scheduling math (no I/O, so it can be tested directly): worker registry, free slots, wait estimate.

The controller keeps the queue; each tick it fills free local slots, then asks the policy about the rest.
"""

from __future__ import annotations

import statistics
import time

from .models import WorkerInfo


class WorkerRegistry:
    """Latest heartbeat of every local worker."""

    def __init__(self, timeout_s: float = 10.0):
        self.timeout_s = timeout_s
        self._workers: dict[str, WorkerInfo] = {}

    def update(self, info: WorkerInfo, now: float | None = None) -> None:
        info.last_seen = now or time.time()
        self._workers[info.id] = info

    def alive(self, now: float | None = None) -> list[WorkerInfo]:
        now = now or time.time()
        return [w for w in self._workers.values() if now - w.last_seen <= self.timeout_s]

    def all(self) -> list[WorkerInfo]:
        return list(self._workers.values())

    def prune(self, now: float | None = None, keep_s: float = 300) -> None:
        """Forget workers that have been silent for keep_s."""
        now = now or time.time()
        self._workers = {k: w for k, w in self._workers.items() if now - w.last_seen <= keep_s}

    def total_slots(self, now: float | None = None) -> int:
        return sum(w.slots for w in self.alive(now))

    def free_slots(self, now: float | None = None) -> int:
        return sum(w.free for w in self.alive(now))


def local_capacity(workers: WorkerRegistry, dispatched_not_started: int, now: float | None = None) -> int:
    """
    Slots we can fill right now. Heartbeats lag behind dispatches, so jobs we already sent
    that no worker has reported as running yet are subtracted.
    """
    return max(0, workers.free_slots(now) - dispatched_not_started)


def estimate_wait_s(position: int, total_slots: int, free_slots: int, runtimes: list[float],
                    default_runtime_s: float = 60.0) -> float | None:
    """
    Rough wait for the job at 0-based queue position `position` if it stays local. The first
    free_slots jobs start now; the rest start as running jobs finish, total_slots at a time,
    each taking the median recent runtime. None when there are no local workers at all.
    """
    if total_slots <= 0:
        return None
    if position < free_slots:
        return 0.0
    runtime = statistics.median(runtimes) if runtimes else default_runtime_s
    return ((position - free_slots) // total_slots + 1) * runtime
