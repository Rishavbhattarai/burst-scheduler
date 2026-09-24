"""Run jobs on AWS Batch.

The job queue and job definition must exist already (e.g. a Fargate or EC2 compute environment).
AWS Batch takes the container image from the job definition, so the job's `image` field is not
used here; command, vCPU and memory are passed as container overrides. Note that Fargate only
accepts certain vCPU/memory combinations (e.g. 0.25 vCPU with 512-2048 MiB).
"""

from __future__ import annotations

from ..models import Job
from .base import CloudBackend, CloudStatus

PENDING = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING"}
DESCRIBE_BATCH = 100  # describe_jobs accepts at most 100 ids


def submit_request(job: Job, job_queue: str, job_definition: str) -> dict:
    """Arguments for batch.submit_job."""
    return {
        "jobName": f"burst-{job.id}",
        "jobQueue": job_queue,
        "jobDefinition": job_definition,
        "containerOverrides": {
            "command": job.command,
            "resourceRequirements": [
                {"type": "VCPU", "value": f"{job.cpu:g}"},
                {"type": "MEMORY", "value": str(job.memory_mb)},
            ],
            "environment": [{"name": "BURST_JOB_ID", "value": job.id}],
        },
        "timeout": {"attemptDurationSeconds": max(60, int(job.timeout_s))},  # Batch minimum is 60 s
        "tags": {"managed-by": "burst-scheduler", "burst-job-id": job.id},
    }


def status_from_description(desc: dict) -> CloudStatus:
    """Map one entry of describe_jobs()['jobs'] to a CloudStatus (Batch times are in milliseconds)."""
    def seconds(key):
        return desc[key] / 1000 if desc.get(key) else None

    state = desc["status"]
    container = desc.get("container", {})
    if state in PENDING:
        return CloudStatus("pending")
    if state == "RUNNING":
        return CloudStatus("running", seconds("startedAt"))
    if state == "SUCCEEDED":
        return CloudStatus("succeeded", seconds("startedAt"), seconds("stoppedAt"),
                           exit_code=container.get("exitCode", 0))
    if state == "FAILED":
        reason = container.get("reason") or desc.get("statusReason")
        return CloudStatus("failed", seconds("startedAt"), seconds("stoppedAt"),
                           exit_code=container.get("exitCode"), error=reason)
    raise ValueError(f"unexpected AWS Batch status {state!r}")


class AwsBatchBackend(CloudBackend):
    kind = "aws_batch"

    def __init__(self, name: str, options: dict, client=None, logs_client=None):
        super().__init__(name, options)
        self.job_queue = options["job_queue"]
        self.job_definition = options["job_definition"]
        if client is None:
            import boto3
            session = boto3.Session(region_name=options.get("region"), profile_name=options.get("profile"))
            client = session.client("batch")
            logs_client = session.client("logs")
        self.client = client
        self.logs = logs_client

    def submit(self, job: Job) -> str:
        return self.client.submit_job(**submit_request(job, self.job_queue, self.job_definition))["jobId"]

    def _log_tail(self, desc: dict) -> str | None:
        stream = desc.get("container", {}).get("logStreamName")
        if not stream or self.logs is None:
            return None
        try:
            events = self.logs.get_log_events(logGroupName="/aws/batch/job", logStreamName=stream,
                                              limit=50, startFromHead=False)["events"]
        except Exception:
            return None
        return "\n".join(e["message"] for e in events)

    def status(self, external_ids: list[str]) -> dict[str, CloudStatus]:
        result = {}
        for i in range(0, len(external_ids), DESCRIBE_BATCH):
            chunk = external_ids[i:i + DESCRIBE_BATCH]
            for desc in self.client.describe_jobs(jobs=chunk)["jobs"]:
                status = status_from_description(desc)
                if status.done:
                    status.output_tail = self._log_tail(desc)
                result[desc["jobId"]] = status
            for missing in set(chunk) - set(result):
                result[missing] = CloudStatus("failed", error="AWS Batch no longer knows this job")
        return result

    def cancel(self, external_id: str) -> None:
        # terminate_job also stops jobs that have not started yet
        self.client.terminate_job(jobId=external_id, reason="cancelled by burst-scheduler")

    def describe(self) -> dict:
        return {**super().describe(), "job_queue": self.job_queue, "job_definition": self.job_definition}
