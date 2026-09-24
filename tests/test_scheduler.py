import pytest

from burst.models import Job, WorkerInfo
from burst.scheduler import WorkerRegistry, estimate_wait_s, local_capacity, plan


def worker(wid: str, slots: int, running: int = 0) -> WorkerInfo:
    return WorkerInfo(id=wid, slots=slots, running=[f"j{i}" for i in range(running)])


def jobs(n: int) -> list[Job]:
    return [Job(command=["true"], submitted_at=i) for i in range(n)]


def test_registry_counts_only_live_workers():
    reg = WorkerRegistry(timeout_s=10)
    reg.update(worker("a", 4, running=1), now=100)
    reg.update(worker("b", 2), now=95)
    assert reg.total_slots(now=100) == 6
    assert reg.free_slots(now=100) == 5
    # b's heartbeat is now 11 s old
    assert [w.id for w in reg.alive(now=106)] == ["a"]
    assert reg.free_slots(now=106) == 3
    reg.prune(now=500, keep_s=300)
    assert reg.all() == []


def test_capacity_subtracts_jobs_dispatched_but_not_started():
    reg = WorkerRegistry()
    reg.update(worker("a", 4, running=1), now=100)
    assert local_capacity(reg, dispatched_not_started=0, now=100) == 3
    assert local_capacity(reg, dispatched_not_started=2, now=100) == 1
    assert local_capacity(reg, dispatched_not_started=10, now=100) == 0


def test_plan_fills_free_slots_in_queue_order():
    reg = WorkerRegistry()
    reg.update(worker("a", 2), now=100)
    reg.update(worker("b", 3, running=2), now=100)
    queued = jobs(10)
    decision = plan(queued, reg, dispatched_not_started=0, now=100)
    assert [p.job.id for p in decision.placements] == [j.id for j in queued[:3]]
    assert all(p.backend == "local" for p in decision.placements)
    assert decision.for_backend("local") == queued[:3]


def test_plan_without_workers_dispatches_nothing():
    assert plan(jobs(3), WorkerRegistry(), 0).placements == []


@pytest.mark.parametrize("position, expected", [
    (0, 0.0),    # free slot now
    (1, 0.0),
    (2, 10.0),   # waits for the first of 4 running jobs to finish
    (5, 10.0),
    (6, 20.0),   # second wave
])
def test_estimate_wait(position, expected):
    assert estimate_wait_s(position, total_slots=4, free_slots=2, runtimes=[8, 10, 12]) == expected


def test_estimate_wait_edge_cases():
    assert estimate_wait_s(0, total_slots=0, free_slots=0, runtimes=[5]) is None
    assert estimate_wait_s(3, total_slots=1, free_slots=0, runtimes=[], default_runtime_s=7) == 28
