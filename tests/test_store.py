from burst.models import Job, JobEvent, JobState
from burst.store import JobStore


def make(store: JobStore, **kw) -> Job:
    return store.add(Job(command=["true"], **kw))


def event(job: Job, kind: str, worker: str = "w1", **kw) -> JobEvent:
    return JobEvent(job_id=job.id, kind=kind, worker=worker, **kw)


def test_add_and_get_roundtrip():
    store = JobStore()
    job = make(store, name="x", priority=5, deadline_s=60, est_runtime_s=3)
    got = store.get(job.id)
    assert got == job
    assert got.command == ["true"]
    assert store.get("nope") is None


def test_queued_order_priority_then_age():
    store = JobStore()
    a = make(store, submitted_at=1, priority=0)
    b = make(store, submitted_at=2, priority=5)
    c = make(store, submitted_at=3, priority=0)
    d = make(store, submitted_at=0.5, priority=5)
    assert [j.id for j in store.queued()] == [d.id, b.id, a.id, c.id]


def test_lifecycle_success():
    store = JobStore()
    job = make(store)
    assert store.mark_dispatched(job.id, "local", now=10)
    assert not store.mark_dispatched(job.id, "local")  # only from queued
    assert store.dispatched_not_started("local") == 1
    assert store.apply_event(event(job, "started", ts=11))
    assert store.dispatched_not_started("local") == 0
    assert store.apply_event(event(job, "finished", ts=15, exit_code=0, output_tail="hi"))
    done = store.get(job.id)
    assert done.state == JobState.SUCCEEDED
    assert (done.backend, done.worker, done.attempts, done.exit_code) == ("local", "w1", 1, 0)
    assert (done.dispatched_at, done.started_at, done.finished_at) == (10, 11, 15)
    assert done.output_tail == "hi"


def test_nonzero_exit_is_failure():
    store = JobStore()
    job = make(store)
    store.mark_dispatched(job.id, "local")
    store.apply_event(event(job, "started"))
    store.apply_event(event(job, "finished", exit_code=2, error="boom"))
    assert store.get(job.id).state == JobState.FAILED
    assert store.get(job.id).error == "boom"


def test_duplicate_events_are_ignored():
    store = JobStore()
    job = make(store)
    store.mark_dispatched(job.id, "local")
    assert store.apply_event(event(job, "started", ts=5))
    assert not store.apply_event(event(job, "started", ts=6))  # same worker again: duplicate
    assert store.apply_event(event(job, "finished", exit_code=0, ts=7))
    assert not store.apply_event(event(job, "finished", exit_code=1, ts=8))
    done = store.get(job.id)
    assert (done.state, done.started_at, done.finished_at, done.attempts) == (JobState.SUCCEEDED, 5, 7, 1)


def test_redelivery_to_another_worker_counts_an_attempt():
    store = JobStore()
    job = make(store)
    store.mark_dispatched(job.id, "local")
    store.apply_event(event(job, "started", worker="w1", ts=5))
    assert store.apply_event(event(job, "started", worker="w2", ts=40))
    again = store.get(job.id)
    assert (again.state, again.worker, again.attempts, again.started_at) == (JobState.RUNNING, "w2", 2, 5)


def test_events_after_cancel_do_not_resurrect():
    store = JobStore()
    job = make(store)
    store.mark_dispatched(job.id, "local")
    assert store.cancel(job.id)
    assert not store.apply_event(event(job, "started"))
    assert not store.apply_event(event(job, "finished", exit_code=0))
    assert store.get(job.id).state == JobState.CANCELLED
    assert not store.cancel(job.id)


def test_counts_and_waits():
    store = JobStore()
    a, b = make(store, submitted_at=100), make(store, submitted_at=100)
    make(store)
    store.mark_dispatched(a.id, "local")
    store.apply_event(event(a, "started", ts=103))
    store.mark_dispatched(b.id, "local")
    store.apply_event(event(b, "started", ts=110))
    store.apply_event(event(b, "finished", ts=130, exit_code=0))
    assert store.count_by_state()["queued"] == 1
    assert store.count_by_state()["running"] == 1
    assert store.count_by_state()["succeeded"] == 1
    assert sorted(store.recent_waits(since_s=60, now=120)) == [3, 10]
    assert store.recent_runtimes() == [20]


def test_list_filters_by_state(tmp_path):
    store = JobStore(str(tmp_path / "jobs.db"))  # also exercises the on-disk (WAL) path
    a = make(store, submitted_at=1)
    make(store, submitted_at=2)
    store.cancel(a.id)
    assert [j.id for j in store.list(JobState.CANCELLED)] == [a.id]
    assert len(store.list()) == 2
    assert len(store.list(limit=1)) == 1
