"""Run jobs as Kubernetes batch/v1 Jobs (one pod, no retries, cleaned up an hour after finishing)."""

from __future__ import annotations

from ..models import Job
from .base import CloudBackend, CloudStatus

LOG_TAIL_LINES = 50


def job_name(job: Job) -> str:
    return f"burst-{job.id}"


def job_manifest(job: Job, namespace: str, image_pull_policy: str = "IfNotPresent",
                 ttl_after_finished_s: int = 3600) -> dict:
    """The Job object we create for `job` (a plain dict, which the client accepts and tests can read)."""
    labels = {"app.kubernetes.io/managed-by": "burst-scheduler", "burst/job-id": job.id}
    resources = {"cpu": str(job.cpu), "memory": f"{job.memory_mb}Mi"}
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name(job), "namespace": namespace, "labels": labels,
                     "annotations": {"burst/job-name": job.name}},
        "spec": {
            "backoffLimit": 0,                                   # retries are the scheduler's decision
            "activeDeadlineSeconds": max(1, int(job.timeout_s)),
            "ttlSecondsAfterFinished": ttl_after_finished_s,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "job",
                        "image": job.image,
                        "imagePullPolicy": image_pull_policy,
                        "command": job.command,
                        "env": [{"name": "BURST_JOB_ID", "value": job.id}],
                        "resources": {"requests": resources, "limits": resources},
                    }],
                },
            },
        },
    }


def _ts(value) -> float | None:
    return value.timestamp() if value is not None else None


def status_from_objects(k8s_job, pod) -> CloudStatus:
    """Map a V1Job (and its pod, if any) to a CloudStatus."""
    st = k8s_job.status
    container = None
    if pod is not None and pod.status and pod.status.container_statuses:
        container = pod.status.container_statuses[0]
    running = container.state.running if container and container.state else None
    terminated = container.state.terminated if container and container.state else None

    if st.succeeded:
        return CloudStatus("succeeded", _ts(terminated.started_at) if terminated else _ts(st.start_time),
                           _ts(st.completion_time) or (_ts(terminated.finished_at) if terminated else None),
                           exit_code=0)
    # Kubernetes >= 1.31 sets FailureTarget first (while the pod is still being terminated) and Failed
    # later; both carry the reason (e.g. DeadlineExceeded)
    failure = [c for c in (st.conditions or []) if c.type in ("Failed", "FailureTarget") and c.status == "True"]
    if st.failed or failure:
        reason = next((f"{c.reason}: {c.message}" for c in failure), None)
        exit_code = terminated.exit_code if terminated else None
        if terminated and terminated.reason and not reason:
            reason = terminated.reason
        return CloudStatus("failed", _ts(terminated.started_at) if terminated else None,
                           _ts(terminated.finished_at) if terminated else None,
                           exit_code=exit_code, error=reason)
    if running is not None:
        return CloudStatus("running", _ts(running.started_at))
    return CloudStatus("pending")


class KubernetesBackend(CloudBackend):
    kind = "kubernetes"

    def __init__(self, name: str, options: dict):
        super().__init__(name, options)
        from kubernetes import client, config

        self.namespace = options.get("namespace", "default")
        self.image_pull_policy = options.get("image_pull_policy", "IfNotPresent")
        if options.get("in_cluster"):
            config.load_incluster_config()
        else:
            config.load_kube_config(config_file=options.get("kubeconfig"), context=options.get("context"))
        self.batch = client.BatchV1Api()
        self.core = client.CoreV1Api()
        self._api_exception = client.exceptions.ApiException

    def submit(self, job: Job) -> str:
        manifest = job_manifest(job, self.namespace, self.image_pull_policy)
        self.batch.create_namespaced_job(self.namespace, manifest)
        return manifest["metadata"]["name"]

    def _pod(self, name: str):
        pods = self.core.list_namespaced_pod(self.namespace, label_selector=f"job-name={name}").items
        return max(pods, key=lambda p: p.metadata.creation_timestamp) if pods else None

    def status(self, external_ids: list[str]) -> dict[str, CloudStatus]:
        result = {}
        for name in external_ids:
            try:
                k8s_job = self.batch.read_namespaced_job_status(name, self.namespace)
            except self._api_exception as exc:
                if exc.status == 404:
                    result[name] = CloudStatus("failed", error="Kubernetes job no longer exists")
                    continue
                raise
            pod = self._pod(name)
            status = status_from_objects(k8s_job, pod)
            if status.done and pod is not None:
                try:
                    # raw response: the client would otherwise turn the log bytes into "b'...'" text
                    response = self.core.read_namespaced_pod_log(
                        pod.metadata.name, self.namespace, tail_lines=LOG_TAIL_LINES, _preload_content=False)
                    status.output_tail = response.data.decode(errors="replace")
                except self._api_exception:
                    pass
            result[name] = status
        return result

    def cancel(self, external_id: str) -> None:
        try:
            self.batch.delete_namespaced_job(external_id, self.namespace, propagation_policy="Background")
        except self._api_exception as exc:
            if exc.status != 404:
                raise

    def describe(self) -> dict:
        return {**super().describe(), "namespace": self.namespace}
