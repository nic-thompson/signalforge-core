from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Literal


SCHEMA_VERSION = "0.1"


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


@dataclass(frozen=True)
class StructuredError:
    """
    Canonical machine-readable error payload.

    Enables:
    - retry orchestration
    - dataset diagnostics
    - pipeline routing decisions
    - observability integrations

    Designed to be safely serialisable and ML-dataset friendly.
    """

    error_code: str
    message: str

    exception_type: str | None = None
    stack_trace: str | None = None

    severity: LogLevel | None = None
    retryable: bool | None = None
    origin: str | None = None

    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class LogEventSchema:
    """
    Canonical structured telemetry event envelope.

    Design goals:
    - machine-readable classification
    - replay-safe schema evolution
    - trace propagation compatibility
    - dataset generation support
    - pipeline-stage observability

    IMPORTANT:
    timestamp must be timezone-aware and in UTC.
    """

    # Temporal context
    timestamp: datetime

    # Severity classification
    level: LogLevel

    # Service identity
    service: str
    environment: str

    # Event classification
    event_type: str
    message: str

    # Distributed tracing context
    trace_id: str | None = None
    parent_trace_id: str | None = None
    correlation_id: str | None = None

    # Pipeline execution context
    pipeline_stage: str | None = None

    # Structured failure payload
    error: StructuredError | None = None

    # Extensible metadata channel
    metadata: Mapping[str, Any] = field(default_factory=dict)

    # Schema evolution support (non-overridable)
    schema_version: str = field(default=SCHEMA_VERSION, init=False)