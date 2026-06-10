"""
signal_forge.alerts.eventbridge_alert_sink

EventBridge implementation of the ``AlertSink`` protocol.

Publishes alerts to an environment-scoped EventBridge bus via
``put_events``. The bus and its routing rules are provisioned upstream
in ``aws-event-pipeline-infra``; this sink only publishes well-formed
alert events. Severity rides on the event's ``DetailType`` so the
upstream EventBridge rules can route CRITICAL and WARNING to different
targets (paging vs digest) without this sink knowing the topology — the
same producer/infra boundary the dataset writer keeps with S3.

boto3 lives here and only here. ``InMemoryAlertSink`` and the protocol
stay dependency-free so they import in any context (Lambda, tests,
replay drivers without AWS deps). The default client factory imports
boto3 lazily, so importing this module does not require boto3 present.

Configuration and replay isolation
-----------------------------------
The sink reads its target bus from ``PlatformSettings.alert_bus`` and is
replay-oblivious: ``PlatformSettings.for_replay()`` swaps the active bus
to the replay-isolated one *before* the sink is constructed, so the sink
never knows which mode it is in. When ``alert_bus`` is ``None`` —
including a replay run whose replay bus was never configured — the sink
is a no-op: it constructs no client and ``publish()`` does nothing. That
honours ``for_replay()``'s "no-op rather than publishing to the live
bus" safety property.

Delivery semantics
------------------
``put_events`` is at-least-once: the request may be retried, and a batch
may partially fail (the response carries a per-entry ``FailedEntryCount``).
This sink delivers faithfully; deduplication is the consumer's job, made
possible by each alert's replay-stable ``alert_id``. EventBridge caps a
single ``put_events`` call at 10 entries, so alerts are chunked.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from event_schema_contracts.alerts.alert_event import AlertEvent

from signal_forge.config.platform_settings import PlatformSettings

# EventBridge caps a single put_events call at 10 entries.
_PUT_EVENTS_MAX_BATCH = 10

# The event Source under which all SignalForge alerts are published.
# Infra's routing rules match on (Source, DetailType).
_ALERT_EVENT_SOURCE = "signalforge.alerts"

# The boto3 events client is untyped under our mypy config; the alias
# documents intent at the seam where a client is injected (tests supply a
# moto-backed client; production uses the default factory below).
EventsClientFactory = Callable[[], Any]


def _default_events_client() -> Any:
    # Imported lazily so this module imports without boto3 present.
    import boto3

    return boto3.client("events")


def _chunk(items: Sequence[AlertEvent], size: int) -> list[list[AlertEvent]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


class EventBridgeAlertSink:
    """
    Alert sink that publishes alerts to an EventBridge bus. Satisfies the
    ``AlertSink`` Protocol.

    Construct with platform settings and, optionally, a client factory.
    The factory seam lets tests inject a moto-backed client; production
    omits it and gets a real boto3 client. The sink assumes the bus
    already exists (provisioned upstream); it neither creates the bus nor
    configures its rules.
    """

    def __init__(
        self,
        *,
        settings: PlatformSettings,
        client_factory: EventsClientFactory | None = None,
    ) -> None:
        self._bus = settings.alert_bus
        self._client: Any
        if self._bus is None:
            # No alert routing configured: no client, publish() no-ops.
            self._client = None
        else:
            factory = client_factory or _default_events_client
            self._client = factory()

    def publish(self, alerts: Sequence[AlertEvent]) -> None:
        if self._bus is None or not alerts:
            return
        for batch in _chunk(alerts, _PUT_EVENTS_MAX_BATCH):
            entries = [self._entry(alert) for alert in batch]
            self._client.put_events(Entries=entries)

    def _entry(self, alert: AlertEvent) -> dict[str, str]:
        payload = alert.payload
        # DetailType carries severity and detection type so infra's rules
        # can route without parsing Detail: "<severity>:<detection_type>",
        # e.g. "CRITICAL:store.outage".
        detail_type = f"{payload.severity.value}:{payload.detection_type}"
        return {
            "Source": _ALERT_EVENT_SOURCE,
            "DetailType": detail_type,
            "Detail": alert.model_dump_json(),
            "EventBusName": self._bus,
        }
