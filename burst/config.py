"""Settings, read from BURST_* environment variables."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field, fields

from .policy import PolicyConfig


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
    # how often cloud jobs are polled for status
    cloud_poll_interval_s: float = field(default_factory=lambda: float(_env("CLOUD_POLL_INTERVAL", "2")))
    # optional TOML file with [policy] and [backends.<name>] sections (see burst.example.toml)
    config_path: str = field(default_factory=lambda: _env("CONFIG", ""))
    policy: PolicyConfig = field(default_factory=PolicyConfig)
    backends: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.config_path:
            self.load_file(self.config_path)

    def load_file(self, path: str) -> None:
        with open(path, "rb") as fh:
            data = tomllib.load(fh)
        known = {f.name for f in fields(PolicyConfig)}
        unknown = set(data.get("policy", {})) - known
        if unknown:
            raise ValueError(f"unknown [policy] settings in {path}: {sorted(unknown)}")
        self.policy = PolicyConfig(**data.get("policy", {}))
        self.backends = {name: opts for name, opts in data.get("backends", {}).items()
                         if opts.get("enabled", True)}
