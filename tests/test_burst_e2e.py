"""Cloud bursting end to end: real NATS, controller and local workers, plus the simulated cloud backend."""

import sys

import pytest

from burst.policy import PolicyConfig

from .conftest import Cluster, wait_until

pytestmark = pytest.mark.integration
PY = sys.executable
SIM = {"sim": {"kind": "simulated", "startup_s": 0.2, "max_jobs": 8, "cpu_hour": 0.36, "gb_hour": 0.0}}
SLEEP = [PY, "-c", "import time; time.sleep(30)"]


async def make(nats_url, tmp_path, **policy) -> Cluster:
    cluster = Cluster(nats_url, tmp_path, policy=PolicyConfig(**policy), backends=SIM)
    await cluster.start_controller()
    return cluster


async def test_free_local_slots_are_used_before_the_cloud(nats_url, tmp_path):
    cluster = await make(nats_url, tmp_path, mode="fastest", burst_threshold_s=0)
    try:
        cluster.start_worker("w1", slots=2)
        await cluster.workers_alive(1)
        jobs = [await cluster.submit(command=["true"]) for _ in range(2)]
        for job in jobs:
            done = await cluster.wait_state(job["id"], "succeeded")
            assert (done["backend"], done["decision"], done["cost"]) == ("local", "free local slot", None)
    finally:
        await cluster.close()


async def test_bursts_when_there_are_no_local_workers(nats_url, tmp_path):
    cluster = await make(nats_url, tmp_path)   # balanced mode, 60 s threshold
    try:
        job = await cluster.submit(command=[PY, "-c", "print('from the cloud')"], cpu=2)
        done = await cluster.wait_state(job["id"], "succeeded")
        assert done["backend"] == "sim" and done["worker"] == "sim"
        assert done["external_id"].startswith("sim-")
        assert done["output_tail"].strip() == "from the cloud"
        assert done["price_per_hour"] == pytest.approx(0.72)     # 2 vCPU x $0.36
        assert done["cost_estimate"] > 0 and done["cost"] > 0
        assert "∞" in done["decision"]
    finally:
        await cluster.close()


async def test_spillover_respects_threshold_and_job_limit(nats_url, tmp_path):
    # one local slot, kept busy. Queued jobs are served one per 10 s (default runtime), so the
    # first waiting job expects 10 s (< 15 s threshold) and stays; later ones expect 20 s+ and burst,
    # until the limit of 2 cloud jobs is reached.
    cluster = await make(nats_url, tmp_path, mode="fastest", burst_threshold_s=15, default_runtime_s=10,
                         max_cloud_jobs=2)
    try:
        cluster.start_worker("w1", slots=1)
        await cluster.workers_alive(1)
        blocker = await cluster.submit(command=SLEEP)
        await cluster.wait_state(blocker["id"], "running")
        jobs = [await cluster.submit(name=f"j{i}", command=SLEEP) for i in range(5)]

        async def settled():
            current = [await cluster.job(j["id"]) for j in jobs]
            return current if all(j["decision"] for j in current) and \
                sum(j["backend"] == "sim" for j in current) == 2 else None
        first, second, third, fourth, fifth = await wait_until(settled, timeout=10)

        assert first["state"] == "queued" and "under the 15s threshold" in first["decision"]
        assert second["backend"] == third["backend"] == "sim"
        assert "fastest mode" in second["decision"]
        for job in (fourth, fifth):
            assert job["state"] == "queued" and "limit reached (2)" in job["decision"]

        stats = (await cluster.client.get("/stats")).json()["cloud"]
        assert stats["active"] == 2 and stats["active_by_backend"] == {"sim": 2}
        assert stats["spend_rate_per_hour"] == pytest.approx(0.72)   # 2 jobs x 1 vCPU x $0.36
    finally:
        await cluster.close()


async def test_cheapest_mode_waits_but_a_deadline_forces_a_burst(nats_url, tmp_path):
    cluster = await make(nats_url, tmp_path, mode="cheapest")
    try:
        patient = await cluster.submit(command=["true"])
        urgent = await cluster.submit(command=["true"], deadline_s=5, est_runtime_s=1)
        done = await cluster.wait_state(urgent["id"], "succeeded")
        assert done["backend"] == "sim" and "deadline" in done["decision"]
        waiting = await cluster.job(patient["id"])
        assert waiting["state"] == "queued" and "cheapest mode" in waiting["decision"]
    finally:
        await cluster.close()


async def test_cancel_a_cloud_job(nats_url, tmp_path):
    cluster = await make(nats_url, tmp_path)
    try:
        job = await cluster.submit(command=SLEEP)
        await cluster.wait_state(job["id"], "running")
        cancelled = (await cluster.client.post(f"/jobs/{job['id']}/cancel")).json()
        assert cancelled["state"] == "cancelled" and cancelled["cost"] > 0
        sim = cluster.controller.backends["sim"]
        await wait_until(lambda: not sim._jobs or sim.status(list(sim._jobs)) is not None)
        assert all(s.proc is None or s.proc.poll() is not None for s in sim._jobs.values())
    finally:
        await cluster.close()


async def test_failing_backend_is_skipped_for_a_while(nats_url, tmp_path):
    cluster = await make(nats_url, tmp_path, mode="fastest")
    try:
        def broken(job):
            raise ConnectionError("cloud API unreachable")
        cluster.controller.backends["sim"].submit = broken
        job = await cluster.submit(command=["true"])

        async def reason():
            return (await cluster.job(job["id"]))["decision"]
        assert "submit to sim failed (ConnectionError)" in await wait_until(reason)
        backends = (await cluster.client.get("/backends")).json()
        assert backends[0]["name"] == "sim" and backends[0]["cooling_down"] is True
        assert (await cluster.job(job["id"]))["state"] == "queued"
    finally:
        await cluster.close()


async def test_policy_api(nats_url, tmp_path):
    cluster = await make(nats_url, tmp_path, mode="cheapest")
    try:
        policy = (await cluster.client.get("/policy")).json()
        assert policy["mode"] == "cheapest" and policy["burst_threshold_s"] == 60

        job = await cluster.submit(command=["true"])
        await wait_until(lambda: cluster.controller.store.get(job["id"]).decision)
        assert (await cluster.job(job["id"]))["state"] == "queued"

        # switching to fastest at runtime sends the waiting job to the cloud
        updated = (await cluster.client.put("/policy", json={"mode": "fastest", "max_cloud_jobs": 3})).json()
        assert (updated["mode"], updated["max_cloud_jobs"], updated["daily_budget"]) == ("fastest", 3, 50.0)
        assert (await cluster.wait_state(job["id"], "succeeded"))["backend"] == "sim"

        assert (await cluster.client.put("/policy", json={"mode": "yolo"})).status_code == 422
        assert (await cluster.client.put("/policy", json={"colour": "red"})).status_code == 422
        backends = (await cluster.client.get("/backends")).json()
        assert [(b["name"], b["kind"], b["max_jobs"]) for b in backends] == [("sim", "simulated", 8)]
    finally:
        await cluster.close()
