"""Cloud backends the scheduler can burst to."""

from .base import CloudBackend, CloudStatus

__all__ = ["CloudBackend", "CloudStatus", "create_backend"]


def create_backend(name: str, options: dict) -> CloudBackend:
    """Build a backend from its [backends.<name>] section in burst.toml."""
    kind = options.get("kind", name)
    if kind == "simulated":
        from .simulated import SimulatedBackend
        return SimulatedBackend(name, options)
    if kind == "kubernetes":
        from .kubernetes import KubernetesBackend
        return KubernetesBackend(name, options)
    if kind == "aws_batch":
        from .aws_batch import AwsBatchBackend
        return AwsBatchBackend(name, options)
    raise ValueError(f"unknown backend kind {kind!r} for [backends.{name}]")
