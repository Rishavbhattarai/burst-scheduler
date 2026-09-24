"""Runs against a real Kubernetes cluster (skipped unless one is reachable).

    kind create cluster --name burst
    docker pull busybox:1.36 && kind load docker-image busybox:1.36 --name burst
    BURST_K8S_CONTEXT=kind-burst pytest -m k8s
"""

import os
import time

import pytest

from burst.models import Job
from burst.policy import PolicyConfig

from .conftest import Cluster

pytestmark = pytest.mark.k8s
CONTEXT = os.environ.get("BURST_K8S_CONTEXT", "kind-burst")
IMAGE = "busybox:1.36"
OPTIONS = {"kind": "kubernetes", "context": CONTEXT, "namespace": "default", "startup_s": 5, "max_jobs": 5}


@pytest.fixture(scope="module")
def backend():
    try:
        from kubernetes import client, config
        config.load_kube_config(context=CONTEXT)
        client.CoreV1Api().list_namespace(_request_timeout=3)
    except Exception as exc:
        pytest.skip(f"no Kubernetes cluster for context {CONTEXT!r}: {type(exc).__name__}")
    from burst.backends.kubernetes import KubernetesBackend
    return KubernetesBackend("k8s", OPTIONS)


def run_until_done(backend, job: Job, timeout: float = 120):
    name = backend.submit(job)
    deadline = time.monotonic() + timeout
    seen = set()
    while time.monotonic() < deadline:
        status = backend.status([name])[name]
        seen.add(status.state)
        if status.done:
            return name, status, seen
        time.sleep(1)
    backend.cancel(name)
    raise AssertionError(f"{name} did not finish in {timeout} s")


def test_success_with_output(backend):
    _, status, seen = run_until_done(backend, Job(image=IMAGE, command=["sh", "-c", "echo hello from k8s"],
                                                  cpu=0.1, memory_mb=64))
    assert (status.state, status.exit_code) == ("succeeded", 0)
    assert status.output_tail.strip() == "hello from k8s"
    assert status.started_at is not None and status.finished_at >= status.started_at
    assert "pending" in seen


def test_exit_code(backend):
    _, status, _ = run_until_done(backend, Job(image=IMAGE, command=["sh", "-c", "echo bad >&2; exit 3"],
                                               cpu=0.1, memory_mb=64))
    assert (status.state, status.exit_code) == ("failed", 3)
    assert "bad" in status.output_tail


def test_deadline(backend):
    _, status, _ = run_until_done(backend, Job(image=IMAGE, command=["sleep", "300"], timeout_s=3,
                                               cpu=0.1, memory_mb=64))
    assert status.state == "failed" and "DeadlineExceeded" in status.error


def test_cancel(backend):
    job = Job(image=IMAGE, command=["sleep", "300"], cpu=0.1, memory_mb=64)
    name = backend.submit(job)
    backend.cancel(name)
    backend.cancel(name)   # cancelling twice is fine
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = backend.status([name])[name]
        if status.state == "failed":
            assert "no longer exists" in status.error
            return
        time.sleep(0.5)
    raise AssertionError("job was not removed")


@pytest.mark.integration
async def test_controller_bursts_to_kubernetes(backend, nats_url, tmp_path):
    cluster = Cluster(nats_url, tmp_path, policy=PolicyConfig(mode="fastest"), backends={"k8s": OPTIONS})
    await cluster.start_controller()
    try:
        # no local workers: the job can only run in the cluster
        job = await cluster.submit(image=IMAGE, command=["sh", "-c", "echo burst ok"], cpu=0.1, memory_mb=64)
        done = await cluster.wait_state(job["id"], "succeeded", "failed", timeout=120)
        assert (done["state"], done["backend"], done["external_id"]) == ("succeeded", "k8s", f"burst-{job['id']}")
        assert done["output_tail"].strip() == "burst ok"
        assert done["cost"] > 0
    finally:
        await cluster.close()
