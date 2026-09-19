"""
stream_pipeline.alerts.acknowledgement_registry

Live acknowledgement-state projection built from alert.acknowledgement
events.

The registry subscribes to the EventRouter as a handler for
("alert.acknowledgement", "v1"). Each observed acknowledgement event
records that the referenced alert (by ``alert_id``) is acknowledged,
backing an O(1) query the AlertRouter uses to annotate routed alerts:

- ``is_acknowledged(alert_id)`` — has this alert been acknowledged?

This mirrors ``DeviceRegistry`` exactly: a parameter-less, router-
subscribed *state projection* — a fold over an event stream into a
materialised view, in the event-sourcing sense — not a time-windowed
computation. There is no watermark, no event-time discipline, no late-
event repair; state depends only on the observed event sequence, so it
is replay-deterministic by construction.

The separation from ``AlertRouter`` is deliberate: the registry *is*
the materialised view (which alerts are acknowledged); the router
performs the stream-table join (detections against that view). Keeping
them apart mirrors the DeviceRegistry/detector split and keeps each
component testable in isolation.

Order of registration with the EventRouter matters: register this
component's handler before the alert router (or before any consumer
that queries acknowledgement state), so the projection is current for
each event before downstream consumers run.

Reminder cadence — re-notifying about a still-unacknowledged alert — is
deliberately NOT here. Cadence is wall-clock-driven and lives in an
operational scheduler outside the deterministic path (working note
D-14). This registry answers only "is it acknowledged?", a pure
function of the event stream.
"""

from __future__ import annotations

from uuid import UUID

from stream_pipeline.streaming.event_protocol import TelemetryEvent


class AcknowledgementRegistry:
    """
    Projection of alert.acknowledgement events into a live set of
    acknowledged ``alert_id``s.

    Construction is parameter-less; the registry begins empty and
    accumulates state as acknowledgement events arrive through
    ``observe_acknowledgement``. Querying an unknown alert returns
    ``False`` — an alert nobody has acknowledged is, correctly, not
    acknowledged.

    The registry is idempotent against duplicate acknowledgement events
    (re-observing an acknowledgement for an already-acknowledged alert
    is a no-op). Multiple distinct acknowledgements of the same alert
    (two responders acking the same page) collapse to the same state:
    acknowledged is acknowledged.
    """

    def __init__(self) -> None:
        self._acknowledged: set[UUID] = set()

    def observe_acknowledgement(self, event: TelemetryEvent) -> None:
        """
        Handler for ``alert.acknowledgement`` events. Records the
        referenced alert as acknowledged.

        The router guarantees this is called only for matching event
        types; the payload is therefore an ``AlertAcknowledgementPayload``
        in practice and the access of ``alert_id`` below is type-safe in
        production. The static type of ``event.payload`` here is ``Any``
        because the router's handler signature is the structural
        ``TelemetryEvent`` protocol — the same arrangement
        ``DeviceRegistry.observe_registration`` relies on.
        """
        alert_id: UUID = event.payload.alert_id
        self._acknowledged.add(alert_id)

    def is_acknowledged(self, alert_id: UUID) -> bool:
        """
        Return whether the alert has been acknowledged. Unknown alerts
        are not acknowledged.
        """
        return alert_id in self._acknowledged

    def acknowledged_count(self) -> int:
        """Total acknowledged alerts. Diagnostic."""
        return len(self._acknowledged)
