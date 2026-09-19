"""
stream_pipeline.config.platform_settings

Immutable, environment-driven configuration for the SignalForge analytics
control plane.

Design properties:

- frozen dataclass — settings are immutable once constructed
- explicit defaults — production-safe values, not zeros
- range-validated at construction — fail fast on misconfiguration
- replay-safe — the same env produces the same settings byte-for-byte
- stdlib-only — importable from any context without pydantic/AWS deps

The five parameters mandated by the platform brief are first-class fields
here. Additional knobs (lookup paths, observability toggles) are layered in
as the system grows.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field, replace
from typing import Final

# ---------------------------------------------------------------------------
# Production-safe defaults
# ---------------------------------------------------------------------------
#
# These defaults reflect the operational targets in the platform brief:
#
#   - dashboard freshness within 5 seconds              -> REALTIME_WINDOW_SECONDS
#   - 2 years analytics retention                       -> DATA_RETENTION_DAYS
#   - tolerate late telemetry without poisoning windows -> LATE_EVENT_TOLERANCE_SECONDS
#   - retail-headset offline detection                  -> OFFLINE_THRESHOLD_SECONDS
#   - store outage detection                            -> OUTAGE_THRESHOLD_RATIO
#
# Operators may override every value via environment variables. Defaults are
# chosen to be safe rather than optimal — Phase 2+ tuning will lower them.

DEFAULT_REALTIME_WINDOW_SECONDS: Final[int] = 5
DEFAULT_OFFLINE_THRESHOLD_SECONDS: Final[int] = 300       # 5 minutes
DEFAULT_OUTAGE_THRESHOLD_RATIO: Final[float] = 0.5         # 50% of devices offline
DEFAULT_LATE_EVENT_TOLERANCE_SECONDS: Final[int] = 60
DEFAULT_DATA_RETENTION_DAYS: Final[int] = 730              # 2 years

# ---------------------------------------------------------------------------
# AWS S3 bucket naming rules
# ---------------------------------------------------------------------------
#
# Loose validation — catches obvious typos and misconfiguration at startup
# so a six-hour replay doesn't fail at the first create_bucket call. AWS S3's
# full naming spec is stricter (no consecutive periods, no IP-address-style
# names, no `xn--` prefix); we trust AWS to reject anything that sneaks past
# us at actual create_bucket time.

_BUCKET_NAME_MIN_LENGTH: Final[int] = 3
_BUCKET_NAME_MAX_LENGTH: Final[int] = 63
_BUCKET_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"
)


def _validate_bucket_name(name: str, field_name: str) -> None:
    """
    Validate an S3 bucket name against loose naming rules.

    Catches obvious typos and misconfiguration at startup. ``field_name``
    is interpolated into error messages so the same helper can validate
    both the live and replay bucket fields with clear attribution.
    """
    if len(name) < _BUCKET_NAME_MIN_LENGTH or len(name) > _BUCKET_NAME_MAX_LENGTH:
        raise ValueError(
            f"{field_name} must be {_BUCKET_NAME_MIN_LENGTH}-"
            f"{_BUCKET_NAME_MAX_LENGTH} chars, got {len(name)}: {name!r}"
        )
    if not _BUCKET_NAME_PATTERN.match(name):
        raise ValueError(
            f"{field_name} must be lowercase alphanumeric or hyphens, "
            f"not starting or ending with hyphen: {name!r}"
        )

_BUS_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9._-]+$")
_BUS_NAME_MAX_LENGTH: Final[int] = 256


def _validate_bus_name(name: str, field_name: str) -> None:
    """
    Validate an EventBridge event-bus name against loose naming rules.

    Catches obvious typos at startup. AWS's full spec is stricter; we
    trust AWS to reject anything that sneaks past at put_events time.
    """
    if len(name) < 1 or len(name) > _BUS_NAME_MAX_LENGTH:
        raise ValueError(
            f"{field_name} must be 1-{_BUS_NAME_MAX_LENGTH} chars, "
            f"got {len(name)}: {name!r}"
        )
    if not _BUS_NAME_PATTERN.match(name):
        raise ValueError(
            f"{field_name} must be alphanumeric, dot, hyphen or "
            f"underscore: {name!r}"
        )

_TABLE_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9._-]+$")
_TABLE_NAME_MIN_LENGTH: Final[int] = 3
_TABLE_NAME_MAX_LENGTH: Final[int] = 255


def _validate_table_name(name: str, field_name: str) -> None:
    """
    Validate a DynamoDB table name against AWS naming rules.

    Catches typos at startup. AWS's spec: 3-255 chars, drawn from
    a-z, A-Z, 0-9, dot, dash, underscore.
    """
    if not (_TABLE_NAME_MIN_LENGTH <= len(name) <= _TABLE_NAME_MAX_LENGTH):
        raise ValueError(
            f"{field_name} must be {_TABLE_NAME_MIN_LENGTH}-"
            f"{_TABLE_NAME_MAX_LENGTH} chars, got {len(name)}: {name!r}"
        )
    if not _TABLE_NAME_PATTERN.match(name):
        raise ValueError(
            f"{field_name} must be alphanumeric, dot, dash or "
            f"underscore: {name!r}"
        )

# ---------------------------------------------------------------------------
# Settings object
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PlatformSettings:
    """
    Immutable platform configuration snapshot.

    Frozen so that pipeline code can hold a reference for the lifetime of a
    run without risk of mutation, and so that replay drivers can assert that
    the original-run and replay-run configurations are byte-identical.
    """

    # Streaming aggregation cadence. Drives the tumbling-window size used by
    # dashboard projections.
    realtime_window_seconds: int = DEFAULT_REALTIME_WINDOW_SECONDS

    # A device is considered offline if no telemetry has been received within
    # this window of wall-clock event-time.
    offline_threshold_seconds: int = DEFAULT_OFFLINE_THRESHOLD_SECONDS

    # Store-level outage threshold expressed as the fraction of registered
    # devices that must be offline simultaneously to trigger an outage.
    outage_threshold_ratio: float = DEFAULT_OUTAGE_THRESHOLD_RATIO

    # Maximum allowed lateness for an event relative to the current
    # watermark before the event is dropped from window aggregations. Late
    # events landing inside this tolerance trigger window repair updates.
    late_event_tolerance_seconds: int = DEFAULT_LATE_EVENT_TOLERANCE_SECONDS

    # Total analytics retention horizon. Drives dataset partition lifecycle
    # rules and S3 object expiry policies (consumed in Phase 4).
    data_retention_days: int = DEFAULT_DATA_RETENTION_DAYS

    # Service identity surfaced to structured-logging-python. Carried here so
    # replay drivers can override it (e.g. "stream-pipeline-replay").
    service_name: str = "stream-pipeline"
    environment: str = field(default_factory=lambda: os.environ.get("SF_ENV", "dev"))

    # S3 bucket for live dataset writes (Phase 4). None means "no dataset
    # export configured" — the writer no-ops rather than failing.
    dataset_bucket: str | None = None

    # S3 bucket for replay-isolated dataset writes. for_replay() swaps the
    # active bucket; the writer reads dataset_bucket and is replay-oblivious.
    replay_dataset_bucket: str | None = None

    # EventBridge bus for live alert publication (Phase 5). None means
    # "no alert routing configured" — the alert sink no-ops rather than
    # failing. The router still produces alerts on ProcessingResult.alerts;
    # only their publication to the bus is gated on this being set.
    alert_bus: str | None = None

    # EventBridge bus for replay-isolated alert publication. for_replay()
    # swaps the active bus; a missing replay bus no-ops rather than
    # publishing replay alerts to the live bus (the safer failure mode).
    replay_alert_bus: str | None = None

    # DynamoDB table for live dashboard projections (Phase 6). None means
    # "no projection store configured" — the store no-ops rather than
    # failing. Projections still update in memory; only their persistence
    # to DynamoDB is gated on this being set.
    projection_table: str | None = None

    # DynamoDB table for replay-isolated projections. for_replay() swaps
    # the active table; a missing replay table no-ops rather than writing
    # replay projections to the live table (the safer failure mode).
    replay_projection_table: str | None = None

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        # Range validation — fail fast at construction time so misconfiguration
        # surfaces at import / startup, not three hours into a replay.
        if self.realtime_window_seconds <= 0:
            raise ValueError("realtime_window_seconds must be > 0")

        if self.offline_threshold_seconds <= 0:
            raise ValueError("offline_threshold_seconds must be > 0")

        if not (0.0 < self.outage_threshold_ratio <= 1.0):
            raise ValueError("outage_threshold_ratio must be in (0.0, 1.0]")

        if self.late_event_tolerance_seconds < 0:
            raise ValueError("late_event_tolerance_seconds must be >= 0")

        if self.data_retention_days <= 0:
            raise ValueError("data_retention_days must be > 0")

        if not self.service_name:
            raise ValueError("service_name must be non-empty")

        if not self.environment:
            raise ValueError("environment must be non-empty")

        if self.dataset_bucket is not None:
            _validate_bucket_name(self.dataset_bucket, "dataset_bucket")
        if self.replay_dataset_bucket is not None:
            _validate_bucket_name(self.replay_dataset_bucket, "replay_dataset_bucket")

        if self.alert_bus is not None:
            _validate_bus_name(self.alert_bus, "alert_bus")
        if self.replay_alert_bus is not None:
            _validate_bus_name(self.replay_alert_bus, "replay_alert_bus")

        if self.projection_table is not None:
            _validate_table_name(self.projection_table, "projection_table")
        if self.replay_projection_table is not None:
            _validate_table_name(
                self.replay_projection_table, "replay_projection_table"
            )

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> PlatformSettings:
        """
        Build settings from environment variables.

        ``env`` may be passed explicitly to keep the call deterministic in
        tests; otherwise ``os.environ`` is used.
        """

        source = env if env is not None else dict(os.environ)

        return cls(
            realtime_window_seconds=_int(
                source, "SF_REALTIME_WINDOW_SECONDS", DEFAULT_REALTIME_WINDOW_SECONDS
            ),
            offline_threshold_seconds=_int(
                source, "SF_OFFLINE_THRESHOLD_SECONDS", DEFAULT_OFFLINE_THRESHOLD_SECONDS
            ),
            outage_threshold_ratio=_float(
                source, "SF_OUTAGE_THRESHOLD_RATIO", DEFAULT_OUTAGE_THRESHOLD_RATIO
            ),
            late_event_tolerance_seconds=_int(
                source,
                "SF_LATE_EVENT_TOLERANCE_SECONDS",
                DEFAULT_LATE_EVENT_TOLERANCE_SECONDS,
            ),
            data_retention_days=_int(
                source, "SF_DATA_RETENTION_DAYS", DEFAULT_DATA_RETENTION_DAYS
            ),
            service_name=source.get("SF_SERVICE_NAME", "stream-pipeline"),
            environment=source.get("SF_ENV", "dev"),
            dataset_bucket=source.get("SF_DATASET_BUCKET") or None,
            replay_dataset_bucket=source.get("SF_REPLAY_DATASET_BUCKET") or None,
            alert_bus=source.get("SF_ALERT_BUS") or None,
            replay_alert_bus=source.get("SF_REPLAY_ALERT_BUS") or None,
            projection_table=source.get("SF_PROJECTION_TABLE") or None,
            replay_projection_table=source.get("SF_REPLAY_PROJECTION_TABLE") or None,
        )

    def for_replay(self, replay_environment: str = "replay") -> PlatformSettings:
        """
        Return a copy of these settings tagged for a replay run.

        Replay should run with identical analytical parameters but a distinct
        environment label so log streams / metrics do not collide with the
        live pipeline. The dataset bucket also swaps to the replay-isolated
        bucket — unconditionally, even when ``replay_dataset_bucket`` is
        ``None``. A missing replay bucket surfaces as a no-op writer rather
        than silently writing to the live bucket, which is the safer
        failure mode.

        The alert bus swaps the same way, for the same reason: a replay
        run publishes to the replay bus, or to nothing if no replay bus
        is configured, never to the live bus.

        The projection table swaps the same way, for the same reason: a
        replay run writes projections to the replay table, or to nothing,
        never to the live table.
        """

        return replace(
            self,
            environment=replay_environment,
            dataset_bucket=self.replay_dataset_bucket,
            alert_bus=self.replay_alert_bus,
            projection_table=self.replay_projection_table,
        )


# ---------------------------------------------------------------------------
# Coercion helpers
# ---------------------------------------------------------------------------


def _int(env: dict[str, str], key: str, default: int) -> int:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from exc


def _float(env: dict[str, str], key: str, default: float) -> float:
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be a float, got {raw!r}") from exc
