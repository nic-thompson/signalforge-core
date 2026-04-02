from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping


SCHEMA_VERSION = "0.1"


@dataclass(frozen=True)
class StructuredError:
    """
    Canonical machine-readable error payload.

    Enables downstream retry orchestration, dataset diagnostics,
    and pipeline observability integrations.
    """

    error_code: str
    message: str
    metadata: Mapping[str, Any] | None = None
    severity: str | None = None
    retryable: bool | None = None
    origin: str | None = None

@dataclass(frozen=True)
class LogEventSchema:
    timestamp: datetime
    level: str

    service: str
    environment: str

    event_type: str
    message: str

    trace_id: str | None = None
    parent_trace_id: str | None = None
    correlation_id: str | None = None
    pipeline_stage: str | None = None

    error: StructuredError | None = None

    metadata: Mapping[str, Any] = field(default_factory=dict)

    schema_version: str = SCHEMA_VERSION