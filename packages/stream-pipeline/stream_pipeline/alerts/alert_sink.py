"""
stream_pipeline.alerts.alert_sink

The publication boundary between the deterministic pipeline and the
external alert-delivery world.

``RealtimePipeline.process()`` produces alerts on
``ProcessingResult.alerts`` as return values; it does not publish them.
Publication is a side effect to an external system (EventBridge in
production), and the live-vs-replay choice of *where* alerts go belongs
to the caller — the production driver wires a live sink, the replay
driver wires a sealed one — not to the pipeline. This mirrors how the
feature and detection layers stay pure returns (D-5) and how replay
isolation is a caller-side routing concern, not a pipeline-internal
flag.

``AlertSink`` is the structural protocol every sink satisfies:
``publish(alerts)`` consumes a batch of alerts and delivers them. The
in-memory implementation here proves the publish path and backs tests
without AWS; the boto3 EventBridge sink (a following unit) satisfies the
same protocol, so swapping production delivery for a test or replay sink
is a substitution, not a code change.

Delivery to an external bus is at-least-once: the network may redeliver,
a batch put may partially fail and retry. That is exactly why each alert
carries a replay-stable ``alert_id`` (derived, not minted) — a consumer
deduplicates on it, achieving effectively-once processing on top of
at-least-once delivery. The sink does not itself deduplicate; it
delivers faithfully and lets the idempotency key do its work downstream.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from event_schema_contracts.alerts.alert_event import AlertEvent


@runtime_checkable
class AlertSink(Protocol):
    """
    Structural protocol for alert publication targets.

    A sink consumes a batch of alerts and delivers them somewhere. The
    pipeline never holds a sink; a caller obtains the alerts from
    ``ProcessingResult.alerts`` and hands them to whichever sink the
    current environment calls for.
    """

    def publish(self, alerts: Sequence[AlertEvent]) -> None:
        """Deliver a batch of alerts. May be called with an empty batch."""
        ...


class InMemoryAlertSink:
    """
    Alert sink that records published alerts in memory.

    The no-AWS implementation: backs tests and any caller that wants to
    collect alerts without an external system (a replay driver routing to
    a sealed in-process sink, for instance). Published alerts accumulate
    in ``published`` in the order they were delivered, across calls.

    Publishing an empty batch is a no-op that still counts as a call, so
    tests can distinguish "published nothing" from "never called".
    """

    def __init__(self) -> None:
        self.published: list[AlertEvent] = []
        self.publish_calls: int = 0

    def publish(self, alerts: Sequence[AlertEvent]) -> None:
        self.publish_calls += 1
        self.published.extend(alerts)
