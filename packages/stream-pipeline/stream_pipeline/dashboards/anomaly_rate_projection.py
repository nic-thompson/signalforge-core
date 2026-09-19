"""
stream_pipeline.dashboards.anomaly_rate_projection

A materialised view of *the rate of signal anomalies by signal type over a
rolling window* - the third of Phase 6's three dashboard projections, and
the third view shape. Where OfflineCountProjection is a partitioned gauge
and ActiveOutageProjection is a global current-set, this is a time-bucketed
rate: a fold over the ``signal.anomaly`` detections emitted by
AnomalyDetector, counting distinct anomaly onsets per signal type within
event-time buckets.

Event time, not wall-clock (DDIA Chapter 12, "Reasoning About Time")
--------------------------------------------------------------------
A "rate over the last N minutes" needs a notion of *now*, which is in
tension with the platform's rule that pure components never read the wall
clock (replay determinism, D-2/D-3). The tension is resolved by splitting
the clock to the read path:

- ``observe()`` is pure event-time bucketing. Each anomaly is placed in the
  bucket its ``detected_at`` falls in, with bucket boundaries aligned to the
  Unix epoch exactly as the streaming WindowAggregator aligns windows (D-3),
  so two runs over the same detection stream produce byte-identical bucket
  contents. No clock, no nondeterminism.
- ``count_in_window`` / ``rate_per_minute`` take the caller's *now* as an
  explicit ``as_of`` argument and sum the buckets overlapping
  ``[as_of - window, as_of)``. Production passes ``datetime.now(UTC)`` from
  the dashboard layer; tests and replay pass a fixed instant. The clock
  lives at the edge, never in the fold.

This mirrors D-14's discipline (ack-state is a deterministic event
projection; the wall-clock reminder cadence is fenced outside it). Here the
deterministic projection is the bucketed counts; the wall-clock read is
fenced into the query.

Idempotency (D-17): a set of detection_ids per bucket
-----------------------------------------------------
A rate is a count, and a bare counter is exactly what D-17 forbids: a
redelivered ``signal.anomaly`` (same deterministic ``detection_id``, whether
from at-least-once delivery or a replay re-run) would inflate it
permanently. So each ``(signal_name, bucket)`` holds the *set* of
detection_ids that landed in it; the rate numerator is the set's
cardinality. Adding an id already present is a no-op, so the fold is
idempotent. AnomalyDetector derives detection_id over
``(partition_key, signal_name, window_start, window_end)``, so anomalies
from different stores are distinct ids in the same signal bucket (the rate
aggregates fleet-wide by signal type, per the roadmap) and redeliveries
collapse.

Bucketing is by ``signal_name`` alone, aggregating across all stores -
"anomaly rate by signal type" is a fleet-wide question. The set is
serialised as a sorted JSON array so the persisted bytes are
arrival-order-independent.

No eviction in the fold (decision: eviction-(i))
------------------------------------------------
Unlike the other two projections, anomaly-rate buckets do not empty
themselves - they age out. This projection does NOT evict: ``observe()`` is
a pure bucketing fold with no high-water tracking and no prune-on-write, to
keep it consistent with the other two folds and free of wall-clock or
watermark machinery. The storage bound is owned by the backend: the
DynamoDB store (a later commit) sets a TTL on bucket keys derived from the
brief's retention horizon. The in-memory store does NOT evict and therefore
grows without bound in a long-running non-DynamoDB process - acceptable for
tests (short-lived) and noted as a known limitation; see docs/working-notes.md.

The query reads only the buckets overlapping the requested window
(``window_seconds / bucket_seconds`` point gets), so read cost is bounded by
the window width regardless of how many historical buckets exist.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from typing import Final

from event_schema_contracts.detection import DetectionEvent

from stream_pipeline.dashboards.projection_store import ProjectionStore
from stream_pipeline.detection.types import DETECTION_TYPE_SIGNAL_ANOMALY

#: Event-time bucket width in seconds. Anomalies are grouped into buckets of
#: this width, aligned to the Unix epoch. A module constant, not a setting:
#: the bucket granularity is an internal resolution choice, not an
#: operational parameter, and the rolling window is chosen by the caller at
#: query time.
_BUCKET_SECONDS: Final[int] = 60

#: Default rolling window for a rate query, in seconds. Callers override per
#: dashboard need.
_DEFAULT_WINDOW_SECONDS: Final[int] = 300


class AnomalyRateProjection:
    """
    Rolling-window anomaly-rate-by-signal-type projection.

    Construct with a ``ProjectionStore``. Feed every ``DetectionEvent`` to
    :meth:`observe`; the projection folds ``signal.anomaly`` detections into
    event-time buckets and ignores everything else. Query the rate with
    :meth:`count_in_window` or :meth:`rate_per_minute`, passing the current
    instant as ``as_of``.
    """

    #: Namespaces this projection within the store.
    VIEW: Final[str] = "anomaly_rate"

    def __init__(self, store: ProjectionStore) -> None:
        self._store = store

    def observe(self, detection: DetectionEvent) -> None:
        """
        Fold one detection into the view.

        ``signal.anomaly`` detections are bucketed by ``signal_name`` and the
        event-time bucket their ``detected_at`` falls in; all other detection
        types are ignored. A detection missing ``signal_name`` in details is
        ignored (the anomaly detector always supplies it, but the projection
        does not assume the producer's internals).
        """
        payload = detection.payload
        if payload.detection_type != DETECTION_TYPE_SIGNAL_ANOMALY:
            return

        signal_name = payload.details.get("signal_name")
        if signal_name is None:
            return

        bucket_start = self._bucket_start(payload.detected_at)
        key = self._key(signal_name, bucket_start)
        ids = self._load(key)
        ids.add(str(payload.detection_id))
        self._store.put(self.VIEW, key, json.dumps(sorted(ids)))

    def count_in_window(
        self,
        signal_name: str,
        *,
        as_of: datetime,
        window_seconds: int = _DEFAULT_WINDOW_SECONDS,
    ) -> int:
        """
        Distinct anomaly onsets for ``signal_name`` in the buckets
        overlapping ``[as_of - window_seconds, as_of)``.

        ``as_of`` is the caller's notion of *now* (the wall clock lives here,
        at the read edge, not in the fold). The count is bucket-granular: the
        leading bucket that straddles ``as_of - window_seconds`` is included
        whole.
        """
        start = as_of - timedelta(seconds=window_seconds)
        bucket = self._bucket_start(start)
        total = 0
        while bucket < as_of:
            raw = self._store.get(self.VIEW, self._key(signal_name, bucket))
            if raw is not None:
                total += len(json.loads(raw))
            bucket = bucket + timedelta(seconds=_BUCKET_SECONDS)
        return total

    def rate_per_minute(
        self,
        signal_name: str,
        *,
        as_of: datetime,
        window_seconds: int = _DEFAULT_WINDOW_SECONDS,
    ) -> float:
        """
        Anomaly onsets per minute for ``signal_name`` over the window ending
        at ``as_of``: the windowed count normalised to a per-minute rate.
        """
        count = self.count_in_window(
            signal_name, as_of=as_of, window_seconds=window_seconds
        )
        return count / (window_seconds / 60)

    def _load(self, key: str) -> set[str]:
        raw = self._store.get(self.VIEW, key)
        if raw is None:
            return set()
        return set(json.loads(raw))

    def _bucket_start(self, moment: datetime) -> datetime:
        # Epoch-aligned bucket floor, mirroring the streaming layer's
        # window alignment (D-3): two runs over the same stream produce
        # identical bucket boundaries.
        floored = math.floor(moment.timestamp() / _BUCKET_SECONDS) * _BUCKET_SECONDS
        return datetime.fromtimestamp(floored, tz=UTC)

    def _key(self, signal_name: str, bucket_start: datetime) -> str:
        # signal_name must not contain "|" (the field separator). Signal
        # names are operator-chosen labels ("latency_ms", "error_rate");
        # the constraint is documented rather than enforced.
        return f"{signal_name}|{bucket_start.isoformat()}"
