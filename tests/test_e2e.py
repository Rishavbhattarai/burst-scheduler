"""End-to-end tests: real nats-server, controller and workers running real commands."""

import os
import signal
import sys

import pytest

from .conftest import Cluster, wait_until

pytestmark = pytest.mark.integration
PY = sys.executable


async def test_job_outcomes(cluster):
    cluster.start_worker("w1", slots=4)
    await cluster.workers_alive(1)
    ok = await cluster.submit(name="ok", command=[PY, "-c", "print(6 * 7)"])
    fail = await cluster.submit(name="fail", command=[PY, "-c", "import sys; print('boom'); sys.exit(3)"])
    missing = await cluster.submit(name="missing", command=["definitely-not-a-command-xyz"])
    slow = await cluster.submit(name="slow", command=[PY, "-c", "import time; time.sleep(30)"], timeout_s=0.5)

    ok = await cluster.wait_state(ok["id"], "succeeded")
    assert (ok["exit_code"], ok["output_tail"].strip(), ok["worker"], ok["backend"]) == (0, "42", "w1", "local")
    assert ok["started_at"] >= ok["dispatched_at"] >= ok["submitted_at"]

    fail = await cluster.wait_state(fail["id"], "failed")
    assert (fail["exit_code"], fail["output_tail"].strip()) == (3, "boom")

    missing = await cluster.wait_state(missing["id"], "failed")
    assert missing["exit_code"] == 127 and "could not start command" in missing["error"]

    slow = await cluster.wait_state(slow["id"], "failed")
    assert "timed out" in slow["error"]


async def test_jobs_wait_for_a_free_slot_and_run_by_priority(cluster):
    cluster.start_worker("w1", slots=1)
    await cluster.workers_alive(1)
    blocker = await cluster.submit(name="blocker", command=[PY, "-c", "import time; time.sleep(1)"])
    await cluster.wait_state(blocker["id"], "running")
    low = await cluster.submit(name="low", command=["true"], priority=0)
    high = await cluster.submit(name="high", command=["true"], priority=10)

    # only one slot: both wait in the controller's queue, not in NATS
    assert (await cluster.job(low["id"]))["state"] == "queued"
    low = await cluster.wait_state(low["id"], "succeeded")
    high = await cluster.wait_state(high["id"], "succeeded")
    assert high["started_at"] < low["started_at"]


async def test_cancel_queued_and_running(cluster):
    # no workers yet: the job stays queued and can be cancelled without touching NATS
    queued = await cluster.submit(command=["true"])
    response = await cluster.client.post(f"/jobs/{queued['id']}/cancel")
    assert response.json()["state"] == "cancelled"

    cluster.start_worker("w1", slots=1)
    await cluster.workers_alive(1)
    running = await cluster.submit(command=[PY, "-c", "import time; time.sleep(60)"])
    await cluster.wait_state(running["id"], "running")
    worker = cluster.worker_tasks[0][0]
    response = await cluster.client.post(f"/jobs/{running['id']}/cancel")
    assert response.json()["state"] == "cancelled"
    # the worker kills the process and frees its slot
    await wait_until(lambda: not worker.running, timeout=5)
    assert (await cluster.job(running["id"]))["state"] == "cancelled"

    again = await cluster.client.post(f"/jobs/{running['id']}/cancel")
    assert again.status_code == 409
    assert (await cluster.client.post("/jobs/nope/cancel")).status_code == 404


async def test_crashed_worker_job_is_redelivered(cluster):
    victim = cluster.start_worker_process("victim", slots=1, ack_wait=2)
    await cluster.workers_alive(1)
    job = await cluster.submit(command=[PY, "-c", "import time; time.sleep(1.5)"])
    await cluster.wait_state(job["id"], "running")
    assert (await cluster.job(job["id"]))["worker"] == "victim"

    os.killpg(victim.pid, signal.SIGKILL)  # the worker and its job die without acknowledging
    victim.wait(5)
    cluster.start_worker_process("rescuer", slots=1, ack_wait=2)

    done = await cluster.wait_state(job["id"], "succeeded", timeout=20)
    assert (done["worker"], done["attempts"]) == ("rescuer", 2)


async def test_events_survive_a_controller_restart(cluster):
    cluster.start_worker("w1", slots=1)
    await cluster.workers_alive(1)
    job = await cluster.submit(command=[PY, "-c", "import time; time.sleep(1)"])
    await cluster.wait_state(job["id"], "running")

    await cluster.stop_controller()   # the job finishes while the controller is down
    await wait_until(lambda: not cluster.worker_tasks[0][0].running, timeout=10)
    await cluster.start_controller()  # same database, same durable consumer

    done = await cluster.wait_state(job["id"], "succeeded")
    assert done["exit_code"] == 0


async def test_stats_and_workers_endpoints(cluster):
    cluster.start_worker("w1", slots=2)
    cluster.start_worker("w2", slots=1)
    await cluster.workers_alive(2)
    job = await cluster.submit(command=["true"])
    await cluster.wait_state(job["id"], "succeeded")
    await wait_until(lambda: cluster.controller.workers.free_slots() == 3)

    stats = (await cluster.client.get("/stats")).json()
    assert stats["workers"] == 2 and stats["slots_total"] == 3 and stats["slots_free"] == 3
    assert stats["counts"]["succeeded"] == 1 and stats["queue_depth"] == 0
    assert stats["estimated_wait_s"] == 0.0 and stats["wait_p50_s"] is not None

    workers = (await cluster.client.get("/workers")).json()
    assert sorted(w["id"] for w in workers) == ["w1", "w2"]
    assert all(w["alive"] for w in workers)
    assert (await cluster.client.get("/healthz")).json()["ok"] is True


async def test_validation_and_auth(nats_url, tmp_path):
    cluster = Cluster(nats_url, tmp_path, api_token="s3cret")
    await cluster.start_controller()
    try:
        assert (await cluster.client.post("/jobs", json={"command": []})).status_code == 422
        assert (await cluster.client.post("/jobs", json={"command": ["true"], "cpu": -1})).status_code == 422
        assert (await cluster.client.post("/jobs", json={"command": ["true"]})).status_code == 201
        cluster.client.headers.pop("Authorization")
        assert (await cluster.client.get("/jobs")).status_code == 401
        assert (await cluster.client.get("/healthz")).status_code == 200  # health check stays open
    finally:
        await cluster.close()
