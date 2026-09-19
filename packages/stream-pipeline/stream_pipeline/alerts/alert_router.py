"""
stream_pipeline.alerts.alert_router

Routes detections into alerts.

The router is a thin consumer of ``ProcessingResult.detections`` (the
return-value contract from D-5): given the detections a ``process()``
call produced, it builds one ``AlertEvent`` per detection. This is the
stream-table join at the heart of the alerting layer — the stream of
detections joined against the acknowledgement-state view maintained by
``AcknowledgementRegistry`` — with the join's output annotated by
whether each alert is already acknowledged.

Three properties define the router's contract:

**Pure function of (detection, ack-state).** The router holds no state
of its own. Given the same detections and the same acknowledgement
registry, it produces the same ``AlertEvent``s, every time. All
acknowledgement state lives in the injected registry; all alert
identity is derived, not minted. This is what keeps routing replay-
deterministic (D-5, D-14).

**Stable alert identity.** Each alert's ``alert_id`` is derived as a
UUIDv5 from the detection's ``detection_id`` via the identity spine
(``derive("alert", str(detection_id))``). Two runs over the same
detection stream produce identical alert ids, so a downstream alert
system can deduplicate on ``alert_id`` — the idempotency-key pattern.
The same derivation is what an acknowledgement references, so an ack
emitted for ``alert_id`` resolves the right alert across replays.

**Annotate, never suppress.** An already-acknowledged detection still
produces an ``AlertEvent``; the router stamps ``acknowledged`` into the
payload's ``details`` rather than withholding the alert. Suppression —
declining to re-page about an acknowledged alert — is a wall-clock,
operational decision that belongs to the cadence scheduler outside the
deterministic path (D-14). The router reports the truth (here is an
alert, and here is whether it is acknowledged) and lets the scheduler
decide whether to act. This also dissolves the out-of-order case: an
ack arriving before its alert is routed needs no special handling,
because the router stamps current state at routing time and a later
change is the scheduler's concern.

Severity is carried through from the detection unchanged: the router
does not reclassify. ``CRITICAL`` vs ``WARNING`` drives downstream
paging-vs-digest dispatch, which is a separate concern from this
detection-to-alert mapping.
"""

from __future__ import annotations

from collections.abc import Sequence

from event_schema_contracts.alerts.alert_event import (
    AlertEvent,
    AlertEventPayload,
)
from event_schema_contracts.base.identity import derive
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection.detection_event import DetectionEvent

from stream_pipeline.alerts.acknowledgement_registry import AcknowledgementRegistry


class AlertRouter:
    """
    Builds ``AlertEvent``s from detections, annotated with
    acknowledgement state.

    Constructed with the acknowledgement registry it queries. The
    registry is injected rather than owned so the same registry can be
    shared with other consumers and updated by the EventRouter
    independently — the materialised view and the join that uses it are
    separate components.
    """

    def __init__(self, acknowledgement_registry: AcknowledgementRegistry) -> None:
        self._acknowledgements = acknowledgement_registry

    # Named for this component rather than left to BaseEvent's own
    # auto-injection, which defaults to "unknown" — indistinguishable
    # downstream from a producer that never named itself at all.
    _SOURCE = "stream-pipeline.alert_router"

    def route(self, detections: Sequence[DetectionEvent]) -> list[AlertEvent]:
        """
        Build one ``AlertEvent`` per detection, in order.

        Pure: no side effects, no clock reads, no mutation of the
        router or the registry. The returned list is parallel to the
        input detections.
        """
        return [self._build_alert(detection) for detection in detections]

    def _build_alert(self, detection: DetectionEvent) -> AlertEvent:
        det = detection.payload
        alert_id = derive("alert", str(det.detection_id))
        acknowledged = self._acknowledgements.is_acknowledged(alert_id)

        # Annotate acknowledgement state into details rather than
        # suppressing. details is the opaque, forward-compatible
        # pass-through on the alert contract (mirroring detection's own
        # details); it carries the ack flag without a schema change.
        details: dict[str, object] = {
            "acknowledged": acknowledged,
            "detected_at": det.detected_at.isoformat(),
        }

        payload = AlertEventPayload(
            alert_id=alert_id,
            detection_id=det.detection_id,
            detection_type=det.detection_type,
            severity=det.severity,
            routed_at=detection.event_timestamp,
            store_id=det.store_id,
            device_id=det.device_id,
            summary=det.threshold_breached,
            details=details,
        )

        # Propagate the detection's trace so an operator can chase an
        # alert back to the detection (and onward to the contributing
        # events) through tracing tools. The alert's own envelope
        # event_id is derived from alert_id so it, too, is replay-stable.
        return AlertEvent(
            event_id=derive("event.alert", str(alert_id)),
            event_timestamp=detection.event_timestamp,
            trace=TraceContext(trace_id=detection.trace.trace_id),
            metadata=EventMetadata(
                event_type=AlertEvent.__event_type__,
                schema_version=AlertEvent.__schema_version__,
                source=self._SOURCE,
            ),
            payload=payload,
        )
