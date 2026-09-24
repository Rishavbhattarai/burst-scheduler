"""NATS subjects and JetStream setup.

JetStream streams (persisted, at-least-once):
  BURST_DISPATCH  burst.dispatch.<backend>   jobs sent to a backend (work queue: each message
                                             is delivered to one worker, redelivered if not acked)
  BURST_EVENTS    burst.events.job           started/finished events for the controller

Core NATS (fire and forget; losing one is harmless):
  burst.workers.heartbeat                    worker id, slots and running jobs every few seconds
  burst.control.cancel                       job ids to cancel, broadcast to all workers
"""

from __future__ import annotations

import nats
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig, RetentionPolicy, StreamConfig
from nats.js.errors import BadRequestError

DISPATCH_STREAM = "BURST_DISPATCH"
EVENTS_STREAM = "BURST_EVENTS"

HEARTBEAT = "burst.workers.heartbeat"
CANCEL = "burst.control.cancel"
EVENTS = "burst.events.job"

LOCAL_CONSUMER = "local-workers"
CONTROLLER_CONSUMER = "controller"


def dispatch_subject(backend: str) -> str:
    return f"burst.dispatch.{backend}"


async def connect(url: str, name: str) -> nats.NATS:
    return await nats.connect(url, name=name, max_reconnect_attempts=-1, reconnect_time_wait=1)


async def ensure_streams(js: JetStreamContext) -> None:
    """Create the streams if they do not exist (safe to call from several processes)."""
    streams = [
        StreamConfig(name=DISPATCH_STREAM, subjects=["burst.dispatch.>"], retention=RetentionPolicy.WORK_QUEUE),
        StreamConfig(name=EVENTS_STREAM, subjects=[EVENTS], retention=RetentionPolicy.LIMITS,
                     max_age=7 * 24 * 3600),
    ]
    for config in streams:
        try:
            await js.add_stream(config)
        except BadRequestError:
            await js.update_stream(config)


def local_consumer_config(ack_wait_s: float) -> ConsumerConfig:
    """Shared pull consumer for the local workers: one job goes to one worker."""
    return ConsumerConfig(
        durable_name=LOCAL_CONSUMER,
        filter_subject=dispatch_subject("local"),
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=ack_wait_s,
        max_deliver=3,
    )
