"""
tests/_fixtures/dataset.py

Factory helpers for constructing the records the Phase 4 dataset writer
consumes — WindowEmission, DetectionEvent, WindowedFeatureVectorEvent,
and ProcessingResult — with sensible defaults so individual tests can
override only what they care about.

Used by tests/datasets/test_writer.py and subsequent dataset-layer tests
in Phase 4 commits 6 onwards.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from event_schema_contracts.alerts.alert_event import AlertEvent
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)
from event_schema_contracts.features.windowed_feature_vector import (
    WindowedFeatureVectorEvent,
    WindowedFeatureVectorPayload,
)

from stream_pipeline.streaming.realtime_pipeline import ProcessingResult
from stream_pipeline.streaming.window_aggregator import WindowEmission


def emission(
    *,
    partition_key: str = "store-1",
    aggregation_name: str = "count",
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    value: object = 1,
    event_count: int = 1,
    is_repair: bool = False,
    last_contributing_trace_id: str | None = None,
) -> WindowEmission:
    """Construct a WindowEmission with sensible defaults."""
    if window_start is None:
        window_start = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
    if window_end is None:
        window_end = datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC)
    return WindowEmission(
        partition_key=partition_key,
        aggregation_name=aggregation_name,
        window_start=window_start,
        window_end=window_end,
        value=value,
        event_count=event_count,
        is_repair=is_repair,
        last_contributing_trace_id=last_contributing_trace_id,
    )


def detection(
    *,
    store_id: str = "store-1",
    detection_type: str = "device.offline",
    severity: DetectionSeverity = DetectionSeverity.WARNING,
    event_timestamp: datetime | None = None,
) -> DetectionEvent:
    """Construct a DetectionEvent with sensible defaults."""
    if event_timestamp is None:
        event_timestamp = datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC)
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type=detection_type,
        severity=severity,
        detected_at=event_timestamp,
        store_id=store_id,
        source_event_id=uuid4(),
        threshold_breached="test_threshold",
        details={},
    )
    return DetectionEvent(
        event_timestamp=event_timestamp,
        trace=TraceContext(),
        payload=payload,
    )


def feature(
    *,
    partition_key: str = "store-1",
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    feature_values: dict[str, object] | None = None,
) -> WindowedFeatureVectorEvent:
    """Construct a WindowedFeatureVectorEvent with sensible defaults."""
    if window_start is None:
        window_start = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
    if window_end is None:
        window_end = datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC)
    if feature_values is None:
        feature_values = {"count": 1}
    payload = WindowedFeatureVectorPayload(
        partition_key=partition_key,
        window_start=window_start,
        window_end=window_end,
        feature_values=feature_values,
        feature_version="v1",
    )
    return WindowedFeatureVectorEvent(
        event_timestamp=window_end,
        trace=TraceContext(),
        payload=payload,
    )


def result(
    *,
    emissions: list[WindowEmission] | None = None,
    detections: list[DetectionEvent] | None = None,
    features: list[WindowedFeatureVectorEvent] | None = None,
    alerts: list[AlertEvent] | None = None,
    event_id: str = "test-event",
) -> ProcessingResult:
    """Construct a ProcessingResult holding the three record lists."""
    return ProcessingResult(
        event_id=event_id,
        partition_key=None,
        classification=None,
        handlers_invoked=0,
        handler_failures=0,
        emissions=emissions or [],
        detections=detections or [],
        extraction_failed=False,
        features=features or [],
        alerts=alerts or [],
    )
