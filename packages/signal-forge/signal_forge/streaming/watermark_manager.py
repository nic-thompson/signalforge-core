"""
signal_forge.streaming.watermark_manager

Per-key event-time watermark with allowed-lateness tolerance.

A watermark is a monotonic timestamp per partition key that represents
"the system does not expect to see any further events with
``event_timestamp`` earlier than this." Watermarks decide when downstream
window aggregators are allowed to emit results without waiting for
straggling events forever.

Design properties
-----------------

- **Per-key**: each partition (store_id, device_id, or any caller-defined
  string key) maintains its own watermark. Stores progress independently;
  one offline store does not stall watermarks for the rest of the fleet.
- **Monotonic**: a watermark never goes backwards. Events with
  earlier ``event_timestamp`` than the current per-key high-watermark are
  classified, never retract.
- **Allowed-lateness**: the watermark advances to
  ``max(event_timestamp) - lateness_tolerance``. Events at or after the
  watermark are classified ON_TIME; events between
  ``watermark - lateness_tolerance`` and the watermark are
  LATE_TOLERATED; older events are LATE_DROPPED.
- **Wall-clock-free**: the manager reads only ``event_timestamp`` from
  inbound events. No ``datetime.now()``, no system clock. This is what
  makes replay deterministic.
- **Pure observation**: ``observe()`` mutates internal state and returns
  a classification. It does not emit logs by itself; logging is performed
  by the caller (typically the realtime pipeline) so the trace_id stamping
  policy is centralised there.

Configuration
-------------

Lateness tolerance is supplied at construction. The intended source is
``PlatformSettings.late_event_tolerance_seconds`` from the platform
configuration, which defaults to 60s and corresponds to the brief's
``LATE_EVENT_TOLERANCE_SECONDS`` knob.

A lateness of zero is a valid configuration: it disables late-event
correction entirely. Every event with ``event_timestamp`` earlier than
the current watermark is classified LATE_DROPPED. Useful for strict-
ordering pipelines and replay-validation runs.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

# Sentinel epoch used as the "no events seen yet" baseline. Chosen as
# datetime.min in UTC so that any real telemetry timestamp is strictly
# greater. Frozen as a module constant to avoid per-call construction.
_EPOCH_BASELINE: Final[datetime] = datetime.min.replace(tzinfo=UTC)


class EventClassification(enum.Enum):
    """Outcome of classifying an event against the current watermark."""

    # event_timestamp is at or after the current watermark —
    # safe to include in active window aggregations.
    ON_TIME = "on_time"

    # event_timestamp is older than the watermark but within the
    # configured lateness tolerance — include in aggregations and
    # trigger window repair downstream.
    LATE_TOLERATED = "late_tolerated"

    # event_timestamp is older than (watermark - lateness_tolerance) —
    # drop from aggregations. The dataset layer (Phase 4) may still
    # archive these for forensic replay.
    LATE_DROPPED = "late_dropped"


@dataclass(frozen=True)
class WatermarkObservation:
    """
    Result of a single observe() call.

    Returned to the caller for routing into window aggregators and for
    structured logging. The watermark fields reflect post-observation
    state — i.e. the watermark *after* this event has been incorporated.
    """

    key: str
    classification: EventClassification
    watermark: datetime
    high_event_timestamp: datetime
    lateness: timedelta


class WatermarkManager:
    """
    Per-key monotonic watermark tracker with allowed-lateness classification.

    Parameters
    ----------
    lateness_tolerance_seconds:
        How far behind the high event-time a late event is still tolerated.
        Must be ``>= 0``. Zero disables late-event correction.

    Notes
    -----
    The manager is single-threaded by design. The realtime pipeline runs
    one consumer per partition shard, and the manager is constructed per
    shard. Cross-thread access is not supported and would defeat the
    monotonicity contract.
    """

    def __init__(self, *, lateness_tolerance_seconds: int) -> None:
        if lateness_tolerance_seconds < 0:
            raise ValueError(
                "lateness_tolerance_seconds must be >= 0"
            )
        self._lateness = timedelta(seconds=lateness_tolerance_seconds)

        # Per-key state: highest event_timestamp seen for that key.
        # The watermark for a key is derived as
        # ``high_event_timestamp - lateness``, computed lazily on read.
        self._high_event_ts: dict[str, datetime] = {}

    # ------------------------------------------------------------------
    # Observation
    # ------------------------------------------------------------------

    def observe(self, key: str, event_timestamp: datetime) -> WatermarkObservation:
        """
        Record an event for ``key`` and classify it against the current
        watermark for that key.

        ``event_timestamp`` must be timezone-aware. The streaming layer
        receives events that have already been validated by upstream
        contracts to enforce this; the assertion here is a defence in
        depth against misuse from internal callers.
        """

        if not key:
            raise ValueError("key must be non-empty")
        if event_timestamp.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware")

        # Pre-observation high-watermark and watermark for classification.
        previous_high = self._high_event_ts.get(key, _EPOCH_BASELINE)
        previous_watermark = self._derive_watermark(previous_high)

        classification = self._classify(event_timestamp, previous_watermark)

        # Monotonicity: the high-event-timestamp only ever advances.
        new_high = max(previous_high, event_timestamp)
        self._high_event_ts[key] = new_high

        return WatermarkObservation(
            key=key,
            classification=classification,
            watermark=self._derive_watermark(new_high),
            high_event_timestamp=new_high,
            lateness=self._lateness,
        )

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def watermark_for(self, key: str) -> datetime:
        """
        Return the current watermark for ``key``.

        Returns the epoch baseline if no events have been observed for
        that key yet — callers can treat that as "watermark not yet
        established" by comparing against ``_EPOCH_BASELINE`` or simply
        treating any observation as the first one.
        """
        return self._derive_watermark(
            self._high_event_ts.get(key, _EPOCH_BASELINE)
        )

    def global_watermark(self) -> datetime:
        """
        Return the global (minimum-across-keys) watermark.

        Used by the dataset layer in Phase 4 to decide when historical
        partitions are safe to seal: a partition is sealable once the
        global watermark has advanced past its right edge plus the
        retention horizon.

        Returns the epoch baseline if no keys have been observed.
        """
        if not self._high_event_ts:
            return _EPOCH_BASELINE
        return min(
            self._derive_watermark(ts)
            for ts in self._high_event_ts.values()
        )

    def keys(self) -> list[str]:
        """Return tracked partition keys in sorted order — for tests and introspection."""
        return sorted(self._high_event_ts.keys())

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _derive_watermark(self, high_event_timestamp: datetime) -> datetime:
        # The watermark trails the highest event-timestamp by the lateness
        # tolerance. Events at or after this point are ON_TIME.
        if high_event_timestamp == _EPOCH_BASELINE:
            return _EPOCH_BASELINE
        return high_event_timestamp - self._lateness

    def _classify(
        self,
        event_timestamp: datetime,
        previous_watermark: datetime,
    ) -> EventClassification:
        if event_timestamp >= previous_watermark:
            return EventClassification.ON_TIME

        # Event predates the watermark. Decide whether it is within the
        # lateness budget.
        late_by = previous_watermark - event_timestamp

        # ``late_by`` is strictly positive here. Within the budget the
        # event is tolerated; beyond it, dropped. Note that with
        # lateness=0 every late event falls into LATE_DROPPED, because
        # ``late_by > timedelta(0)`` whenever event_timestamp is older
        # than previous_watermark.
        if late_by <= self._lateness and self._lateness > timedelta(0):
            return EventClassification.LATE_TOLERATED
        return EventClassification.LATE_DROPPED
