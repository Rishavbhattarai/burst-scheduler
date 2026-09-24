"""Fixtures that run the real thing: a nats-server with JetStream, the controller and workers."""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest

from burst.config import Settings
from burst.controller import create_app
from burst.worker import Worker

NATS_SERVER = shutil.which("nats-server")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def wait_until(predicate, timeout: float = 10.0, interval: float = 0.05):
    """Await an async (or plain) predicate until it returns something truthy."""
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return result
        if time.monotonic() > deadline:
            raise AssertionError(f"condition not met within {timeout} s")
        await asyncio.sleep(interval)


@pytest.fixture
def nats_url(tmp_path):
    if NATS_SERVER is None:
        pytest.skip("nats-server not installed")
    port = free_port()
    proc = subprocess.Popen([NATS_SERVER, "-js", "-p", str(port), "-sd", str(tmp_path / "js")],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while True:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            if time.monotonic() > deadline:
                proc.kill()
                raise
            time.sleep(0.05)
    yield f"nats://127.0.0.1:{port}"
    proc.terminate()
    proc.wait(10)


class Cluster:
    """Controller + HTTP client + helpers to start workers, all against one nats-server."""

    def __init__(self, nats_url: str, tmp_path, api_token: str = ""):
        self.nats_url = nats_url
        self.tmp_path = tmp_path
        self.settings = Settings(nats_url=nats_url, db_path=str(tmp_path / "burst.db"), api_token=api_token,
                                 schedule_interval_s=0.05, worker_timeout_s=3)
        self.worker_tasks: list[tuple[Worker, asyncio.Task]] = []
        self.worker_procs: list[subprocess.Popen] = []

    async def start_controller(self) -> None:
        self.app = create_app(self.settings)
        self.controller = self.app.state.controller
        await self.controller.start()
        headers = {"Authorization": f"Bearer {self.settings.api_token}"} if self.settings.api_token else {}
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://test",
                                        headers=headers)

    async def stop_controller(self) -> None:
        await self.client.aclose()
        await self.controller.stop()

    def start_worker(self, worker_id: str, slots: int = 1) -> Worker:
        worker = Worker(self.nats_url, slots, worker_id, heartbeat_s=0.2, shutdown_grace_s=1)
        self.worker_tasks.append((worker, asyncio.create_task(worker.run())))
        return worker

    def start_worker_process(self, worker_id: str, slots: int = 1, ack_wait: float = 2) -> subprocess.Popen:
        env = {**os.environ, "BURST_NATS_URL": self.nats_url, "BURST_ACK_WAIT": str(ack_wait)}
        proc = subprocess.Popen([sys.executable, "-m", "burst.worker", "--slots", str(slots), "--id", worker_id],
                                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                start_new_session=True)
        self.worker_procs.append(proc)
        return proc

    async def workers_alive(self, n: int) -> None:
        await wait_until(lambda: len(self.controller.workers.alive()) >= n)

    async def submit(self, **spec) -> dict:
        response = await self.client.post("/jobs", json=spec)
        assert response.status_code == 201, response.text
        return response.json()

    async def job(self, job_id: str) -> dict:
        return (await self.client.get(f"/jobs/{job_id}")).json()

    async def wait_state(self, job_id: str, *states: str, timeout: float = 15) -> dict:
        async def check():
            job = await self.job(job_id)
            return job if job["state"] in states else None
        return await wait_until(check, timeout)

    async def close(self) -> None:
        for worker, _ in self.worker_tasks:
            worker.stop()
        for _, task in self.worker_tasks:
            await asyncio.wait_for(task, 10)
        for proc in self.worker_procs:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(5)
        await self.stop_controller()


@pytest.fixture
async def cluster(nats_url, tmp_path):
    c = Cluster(nats_url, tmp_path)
    await c.start_controller()
    yield c
    await c.close()
