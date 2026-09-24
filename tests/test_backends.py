import datetime
import sys
import time
from types import SimpleNamespace as NS

import boto3
import pytest
from botocore.stub import Stubber

from burst.backends import Pricing, create_backend
from burst.backends.aws_batch import AwsBatchBackend, status_from_description, submit_request
from burst.backends.kubernetes import job_manifest, status_from_objects
from burst.models import Job

JOB = Job(id="abc123", name="train", command=["python", "-c", "print(1)"], image="python:3.12-slim",
          cpu=2, memory_mb=4096, timeout_s=600)


def test_pricing():
    assert Pricing(cpu_hour=0.05, gb_hour=0.01).per_hour(JOB) == pytest.approx(2 * 0.05 + 4 * 0.01)


def test_unknown_backend_kind():
    with pytest.raises(ValueError):
        create_backend("x", {"kind": "mainframe"})


# -- Kubernetes -------------------------------------------------------------------------------------------

def test_kubernetes_manifest():
    m = job_manifest(JOB, "jobs")
    assert (m["apiVersion"], m["kind"], m["metadata"]["name"], m["metadata"]["namespace"]) == \
        ("batch/v1", "Job", "burst-abc123", "jobs")
    spec = m["spec"]
    assert (spec["backoffLimit"], spec["activeDeadlineSeconds"], spec["ttlSecondsAfterFinished"]) == (0, 600, 3600)
    pod = spec["template"]["spec"]
    assert pod["restartPolicy"] == "Never"
    (container,) = pod["containers"]
    assert container["image"] == "python:3.12-slim" and container["command"] == JOB.command
    assert container["resources"]["requests"] == {"cpu": "2.0", "memory": "4096Mi"}
    assert container["resources"]["limits"] == container["resources"]["requests"]
    assert {"name": "BURST_JOB_ID", "value": "abc123"} in container["env"]
    assert spec["template"]["metadata"]["labels"]["burst/job-id"] == "abc123"


T0 = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
T1 = T0 + datetime.timedelta(seconds=30)


def k8s_job(**status):
    base = {"succeeded": None, "failed": None, "active": None, "conditions": None, "start_time": T0,
            "completion_time": None}
    return NS(status=NS(**{**base, **status}))


def pod(running=None, terminated=None):
    state = NS(running=running, terminated=terminated, waiting=None)
    return NS(status=NS(container_statuses=[NS(state=state)]), metadata=NS(name="p", creation_timestamp=T0))


def test_kubernetes_status_mapping():
    assert status_from_objects(k8s_job(), None).state == "pending"
    assert status_from_objects(k8s_job(active=1), pod()).state == "pending"   # pod created, not running yet

    running = status_from_objects(k8s_job(active=1), pod(running=NS(started_at=T0)))
    assert (running.state, running.started_at) == ("running", T0.timestamp())

    done = status_from_objects(k8s_job(succeeded=1, completion_time=T1),
                               pod(terminated=NS(started_at=T0, finished_at=T1, exit_code=0, reason="Completed")))
    assert (done.state, done.exit_code, done.started_at, done.finished_at) == \
        ("succeeded", 0, T0.timestamp(), T1.timestamp())

    failed = status_from_objects(
        k8s_job(failed=1, conditions=[NS(type="Failed", status="True", reason="BackoffLimitExceeded",
                                         message="Job has reached the specified backoff limit")]),
        pod(terminated=NS(started_at=T0, finished_at=T1, exit_code=3, reason="Error")))
    assert (failed.state, failed.exit_code) == ("failed", 3)
    assert failed.error.startswith("BackoffLimitExceeded")

    # Kubernetes >= 1.31: failed=1 with only FailureTarget while the pod is being terminated
    target = status_from_objects(
        k8s_job(failed=1, conditions=[NS(type="FailureTarget", status="True", reason="DeadlineExceeded",
                                         message="Job was active longer than specified deadline")]), None)
    assert (target.state, target.error) == ("failed", "DeadlineExceeded: Job was active longer than specified deadline")

    # deadline exceeded: the pod is gone, only the condition says why
    deadline = status_from_objects(
        k8s_job(conditions=[NS(type="Failed", status="True", reason="DeadlineExceeded", message="too slow")]), None)
    assert (deadline.state, deadline.exit_code, deadline.error) == ("failed", None, "DeadlineExceeded: too slow")


# -- AWS Batch -------------------------------------------------------------------------------------------

def test_batch_submit_request():
    req = submit_request(JOB, "q", "def:3")
    assert (req["jobName"], req["jobQueue"], req["jobDefinition"]) == ("burst-abc123", "q", "def:3")
    overrides = req["containerOverrides"]
    assert overrides["command"] == JOB.command
    assert {"type": "VCPU", "value": "2"} in overrides["resourceRequirements"]
    assert {"type": "MEMORY", "value": "4096"} in overrides["resourceRequirements"]
    assert req["timeout"] == {"attemptDurationSeconds": 600}
    short = Job(command=["true"], timeout_s=5)
    assert submit_request(short, "q", "d")["timeout"] == {"attemptDurationSeconds": 60}


@pytest.mark.parametrize("status, state", [("SUBMITTED", "pending"), ("RUNNABLE", "pending"),
                                           ("STARTING", "pending"), ("RUNNING", "running")])
def test_batch_status_in_progress(status, state):
    assert status_from_description({"status": status, "startedAt": 1_000_000}).state == state


def test_batch_status_finished():
    ok = status_from_description({"status": "SUCCEEDED", "startedAt": 1_000_000, "stoppedAt": 1_030_000,
                                  "container": {"exitCode": 0}})
    assert (ok.state, ok.exit_code, ok.started_at, ok.finished_at) == ("succeeded", 0, 1000.0, 1030.0)
    bad = status_from_description({"status": "FAILED", "statusReason": "Job attempt duration exceeded timeout"})
    assert (bad.state, bad.exit_code, bad.error) == ("failed", None, "Job attempt duration exceeded timeout")
    oom = status_from_description({"status": "FAILED", "container": {"exitCode": 137, "reason": "OutOfMemoryError"}})
    assert (oom.exit_code, oom.error) == (137, "OutOfMemoryError")


def test_batch_backend_calls_the_api_correctly():
    client = boto3.client("batch", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x")
    backend = AwsBatchBackend("aws", {"job_queue": "q", "job_definition": "d"}, client=client, logs_client=None)
    with Stubber(client) as stub:
        stub.add_response("submit_job", {"jobId": "id-1", "jobName": "burst-abc123", "jobArn": "arn:x"},
                          submit_request(JOB, "q", "d"))
        assert backend.submit(JOB) == "id-1"

        stub.add_response("describe_jobs", {"jobs": [{
            "jobId": "id-1", "jobName": "burst-abc123", "jobQueue": "q", "jobDefinition": "d",
            "status": "SUCCEEDED", "startedAt": 1_000_000, "stoppedAt": 1_002_000,
            "container": {"exitCode": 0}}]}, {"jobs": ["id-1", "id-gone"]})
        result = backend.status(["id-1", "id-gone"])
        assert result["id-1"].state == "succeeded"
        assert result["id-gone"].state == "failed" and "no longer knows" in result["id-gone"].error

        stub.add_response("terminate_job", {}, {"jobId": "id-1", "reason": "cancelled by burst-scheduler"})
        backend.cancel("id-1")
        stub.assert_no_pending_responses()


# -- simulated -------------------------------------------------------------------------------------------

def wait_for(backend, external_id, *states, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = backend.status([external_id])[external_id]
        if st.state in states:
            return st
        time.sleep(0.05)
    raise AssertionError(f"never reached {states}")


def test_simulated_backend_lifecycle():
    sim = create_backend("sim", {"kind": "simulated", "startup_s": 0.3, "cpu_hour": 1, "gb_hour": 0})
    ok = sim.submit(Job(command=[sys.executable, "-c", "print('hi')"]))
    assert sim.status([ok])[ok].state == "pending"   # still "provisioning"
    st = wait_for(sim, ok, "succeeded", "failed")
    assert (st.state, st.exit_code, st.output_tail.strip()) == ("succeeded", 0, "hi")
    assert st.started_at is not None

    bad = sim.submit(Job(command=[sys.executable, "-c", "raise SystemExit(4)"]))
    assert wait_for(sim, bad, "succeeded", "failed").exit_code == 4

    missing = sim.submit(Job(command=["no-such-command-xyz"]))
    st = wait_for(sim, missing, "succeeded", "failed")
    assert st.exit_code == 127 and "could not start" in st.error

    slow = sim.submit(Job(command=["sleep", "30"], timeout_s=0.3))
    assert "timed out" in wait_for(sim, slow, "succeeded", "failed").error

    victim = sim.submit(Job(command=["sleep", "30"]))
    wait_for(sim, victim, "running")
    sim.cancel(victim)
    assert wait_for(sim, victim, "failed").error == "cancelled"


def test_simulated_backend_close_stops_its_processes():
    sim = create_backend("sim", {"kind": "simulated", "startup_s": 0})
    external_id = sim.submit(Job(command=["sleep", "30"]))
    wait_for(sim, external_id, "running")
    proc = next(iter(sim._jobs.values())).proc
    sim.close()
    assert proc.poll() is not None and sim._jobs == {}
