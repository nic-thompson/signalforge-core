"""
Lightweight test fixtures that satisfy the streaming-layer protocols.

These doubles let the streaming, router, and aggregator tests run without
requiring the upstream ``event-schema-contracts`` (pydantic) dependency to
be installed in the test environment. Real production runs always use
upstream ``BaseEvent`` instances, which satisfy the same protocols
structurally.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import UUID, uuid4


@dataclass(frozen=True)
class FakeMetadata:
    schema_version: str
    event_type: str
    source: str = "test-source"


@dataclass(frozen=True)
class FakeTrace:
    trace_id: UUID = field(default_factory=uuid4)
    root_trace_id: UUID | None = None


@dataclass(frozen=True)
class FakeEvent:
    """
    Minimal stand-in for ``event_schema_contracts.base.BaseEvent``.

    Satisfies ``signal_forge.streaming.event_protocol.TelemetryEvent``.
    """

    event_type: str
    schema_version: str
    event_timestamp: datetime
    payload: Any = None
    event_id: UUID = field(default_factory=uuid4)
    ingest_timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    trace: FakeTrace = field(default_factory=FakeTrace)
    metadata: FakeMetadata = field(init=False)

    def __post_init__(self) -> None:
        # Build metadata from the convenience fields so test setup is short.
        object.__setattr__(
            self,
            "metadata",
            FakeMetadata(
                schema_version=self.schema_version,
                event_type=self.event_type,
            ),
        )


# ---------------------------------------------------------------------------
# Recording logger — captures log calls for assertions
# ---------------------------------------------------------------------------


@dataclass
class RecordedLog:
    level: str
    message: str
    event_type: str
    metadata: Mapping[str, Any]
    trace_id: str | None


class RecordingLogger:
    """
    Captures structured-log emissions for inspection in tests.

    Implements the ``StructuredLoggerLike`` protocol exactly.
    """

    def __init__(self) -> None:
        self.records: list[RecordedLog] = []

    def info(
        self,
        message: str,
        event_type: str = "log.info",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        self.records.append(
            RecordedLog("INFO", message, event_type, dict(metadata or {}), trace_id)
        )

    def warning(
        self,
        message: str,
        event_type: str = "log.warning",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        self.records.append(
            RecordedLog("WARNING", message, event_type, dict(metadata or {}), trace_id)
        )

    def error(
        self,
        message: str,
        event_type: str = "log.error",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        self.records.append(
            RecordedLog("ERROR", message, event_type, dict(metadata or {}), trace_id)
        )

    def by_event_type(self, event_type: str) -> list[RecordedLog]:
        return [r for r in self.records if r.event_type == event_type]