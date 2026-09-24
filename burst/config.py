"""Settings, read from BURST_* environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(f"BURST_{name}", default)


@dataclass
class Settings:
    nats_url: str = field(default_factory=lambda: _env("NATS_URL", "nats://127.0.0.1:4222"))
    db_path: str = field(default_factory=lambda: _env("DB_PATH", "burst.db"))
    api_token: str = field(default_factory=lambda: _env("API_TOKEN", ""))
    # how often the scheduler looks at the queue
    schedule_interval_s: float = field(default_factory=lambda: float(_env("SCHEDULE_INTERVAL", "0.5")))
    # a worker without a heartbeat for this long is considered gone
    worker_timeout_s: float = field(default_factory=lambda: float(_env("WORKER_TIMEOUT", "10")))
