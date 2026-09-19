"""
stream_pipeline.streaming.window_aggregator

Event-time tumbling and sliding window aggregator with late-event repair.

The aggregator consumes a stream of (partition_key, event_timestamp,
contribution, classification, watermark) observations and emits window
results when the watermark advances past a window's right edge.

Design properties
-----------------

- **Event-time semantics**: windows are bucketed by ``event_timestamp``,
  not by arrival order. Watermark-driven emission ensures correctness
  under late and out-of-order arrivals.
- **Epoch-relative alignment**: window boundaries are
  ``floor(t / slide) * slide`` measured from the Unix epoch, never from
  the first event seen. This guarantees replay determinism and cross-
  shard window alignment for downstream joins.
- **Pluggable aggregations**: an ``Aggregation`` is an init/combine pair.
  Phase 1 ships ``CountAggregation`` and ``SumAggregation``; Phase 2
  added ``DistinctCountAggregation``; Phase 3 adds ``MeanAggregation``.
  Further aggregations (quantiles, etc.) can be added without
  modifying the aggregator core.
- **Late-event window repair**: a ``LATE_TOLERATED`` event whose
  timestamp falls inside a still-retained window updates that window's
  state and triggers a re-emission tagged ``is_repair=True``.
- **Sealing horizon**: window state is retained until the watermark
  advances past ``window_end + lateness_tolerance``; afterwards, the
  window is sealed and any further late event for it is silently
  ignored (the upstream watermark manager will have already classified
  it ``LATE_DROPPED`` in any normal case).
- **Per-key isolation**: ``(partition_key, window_start)`` is the state
  key. One key's late event never affects another key's emission cadence.
- **Deterministic emission order**: when multiple windows close on the
  same observation, they emit in chronological window-start order.

Tumbling windows are the special case where ``size == slide``.
"""

from __future__ import annotations

import bisect
import dataclasses
import math
from collections.abc import Callable, Hashable
from datetime import UTC, datetime, timedelta
from typing import Any, Generic, Protocol, TypeVar

from stream_pipeline.streaming.watermark_manager import EventClassification

# Epoch baseline used for boundary arithmetic. Windows are aligned to
# (event_timestamp - epoch).total_seconds() // slide_seconds.
_EPOCH: datetime = datetime(1970, 1, 1, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Window specification
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class WindowSpec:
    """
    Defines window geometry.

    A tumbling window is the special case ``size == slide``. A sliding
    window has ``slide < size``. ``slide > size`` is rejected because it
    would create gaps where events belong to no window — almost always
    a configuration mistake.
    """

    size_seconds: int
    slide_seconds: int

    def __post_init__(self) -> None:
        if self.size_seconds <= 0:
            raise ValueError("size_seconds must be > 0")
        if self.slide_seconds <= 0:
            raise ValueError("slide_seconds must be > 0")
        if self.slide_seconds > self.size_seconds:
            raise ValueError(
                "slide_seconds must be <= size_seconds (gapped windows not permitted)"
            )
        # Require slide to evenly divide size. Otherwise window boundaries
        # drift relative to each other across slides and cross-shard
        # joins become unreliable.
        if self.size_seconds % self.slide_seconds != 0:
            raise ValueError(
                "size_seconds must be an integer multiple of slide_seconds"
            )

    @property
    def is_tumbling(self) -> bool:
        return self.size_seconds == self.slide_seconds

    @property
    def size(self) -> timedelta:
        return timedelta(seconds=self.size_seconds)

    @property
    def slide(self) -> timedelta:
        return timedelta(seconds=self.slide_seconds)


# ---------------------------------------------------------------------------
# Aggregation strategy
# ---------------------------------------------------------------------------


StateT = TypeVar("StateT")
ResultT = TypeVar("ResultT", covariant=True)


class Aggregation(Protocol, Generic[StateT, ResultT]):
    """
    Pluggable aggregation strategy.

    An aggregation is an ``initial`` factory plus a ``combine`` function.
    The aggregator never mutates state in place; ``combine`` returns a
    new state. This is what enables window repair: re-applying an
    event to a window's state is just another ``combine`` call.

    ``finalise`` converts internal state into the externally-emitted
    value. For ``count`` and ``sum`` the state and result are the same;
    for ``mean`` they differ (state = (sum, n), result = sum / n).
    """

    name: str

    def initial(self) -> StateT: ...
    def combine(self, state: StateT, contribution: Any) -> StateT: ...
    def finalise(self, state: StateT) -> ResultT: ...


@dataclasses.dataclass(frozen=True)
class CountAggregation:
    """
    Counts events. ``contribution`` is ignored — every event contributes 1.
    Useful for ``active_devices_per_store``, ``help_button_rate``, etc.
    """

    name: str = "count"

    def initial(self) -> int:
        return 0

    def combine(self, state: int, contribution: Any) -> int:
        return state + 1

    def finalise(self, state: int) -> int:
        return state


@dataclasses.dataclass(frozen=True)
class SumAggregation:
    """
    Sums numeric ``contribution`` values. Used for accumulating numeric
    payload fields (e.g. session durations, byte counts).
    """

    name: str = "sum"

    def initial(self) -> float:
        return 0.0

    def combine(self, state: float, contribution: Any) -> float:
        if not isinstance(contribution, (int, float)):
            raise TypeError(
                f"SumAggregation contribution must be numeric, got {type(contribution).__name__}"
            )
        return state + float(contribution)

    def finalise(self, state: float) -> float:
        return state


@dataclasses.dataclass(frozen=True)
class DistinctCountAggregation:
    """
    Counts distinct values extracted from contributions. Useful for
    ``distinct_devices_per_store`` and similar cardinality measures.

    The ``key`` callable maps each contribution to a hashable value;
    the aggregation's state is the set of keys seen so far in the
    window, and finalisation returns its cardinality.

    The explicit ``key`` parameter forces callers to declare what is
    being counted (which field of which payload), rather than relying
    on payload equality semantics that can drift silently as schemas
    evolve. If two payloads should count as the same device, they
    should produce the same ``key(payload)`` value.
    """

    key: Callable[[Any], Hashable]
    name: str = "distinct_count"

    def initial(self) -> set[Hashable]:
        return set()

    def combine(self, state: set[Hashable], contribution: Any) -> set[Hashable]:
        # Mutate in place and return — the aggregator stores whatever we
        # return as the next state. CountAggregation and SumAggregation
        # construct new values; sets allow O(1) update without
        # allocation churn at fleet scale.
        state.add(self.key(contribution))
        return state

    def finalise(self, state: set[Hashable]) -> int:
        return len(state)


@dataclasses.dataclass(frozen=True)
class MeanAggregation:
    """
    Computes the windowed mean of numeric contributions: sum / count.

    State is a (total, count) tuple. ``finalise`` returns 0.0 for an
    empty window — by convention, ``WindowEmission.event_count``
    carries the empty-window discriminator. Downstream consumers
    that need to distinguish "the mean was zero" from "no
    contributions" check ``event_count > 0`` before trusting the
    value. This mirrors how statistical tools handle empty samples:
    the result is technically undefined, but the sample size carries
    the signal.

    Non-numeric contributions raise ``TypeError``, matching
    ``SumAggregation``'s precedent.
    """

    name: str = "mean"

    def initial(self) -> tuple[float, int]:
        return (0.0, 0)

    def combine(
        self, state: tuple[float, int], contribution: Any
    ) -> tuple[float, int]:
        if not isinstance(contribution, (int, float)):
            raise TypeError(
                f"MeanAggregation contribution must be numeric, "
                f"got {type(contribution).__name__}"
            )
        total, count = state
        return (total + float(contribution), count + 1)

    def finalise(self, state: tuple[float, int]) -> float:
        total, count = state
        return total / count if count > 0 else 0.0


# ---------------------------------------------------------------------------
# Emission record
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class WindowEmission:
    """
    A single window result emitted by the aggregator.

    Emitted both for initial closure (when the watermark first crosses
    the window's right edge) and for late-event repair (when a
    LATE_TOLERATED event updates an already-emitted window).

    ``last_contributing_trace_id`` carries the ``trace_id`` of the most
    recent event that contributed to the window. Phase 2 emission
    detectors use this to propagate trace lineage from contributing
    telemetry events to derived ``DetectionEvent``s. ``None`` when the
    aggregator was called without a ``trace_id`` (e.g. from tests that
    do not exercise trace propagation).
    """

    partition_key: str
    aggregation_name: str
    window_start: datetime
    window_end: datetime
    value: Any
    event_count: int
    is_repair: bool
    last_contributing_trace_id: str | None = None


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _WindowState:
    """Internal per-window state. Mutated by the aggregator only."""

    state: Any
    event_count: int
    has_emitted: bool
    last_contributing_trace_id: str | None = None


class WindowAggregator:
    """
    Event-time tumbling/sliding window aggregator with late-event repair.

    Parameters
    ----------
    spec:
        Window geometry (size, slide). Use ``size == slide`` for tumbling.
    aggregation:
        Aggregation strategy. Phase 1 provides ``CountAggregation`` and
        ``SumAggregation``.
    lateness_tolerance_seconds:
        Sealing horizon. State for an emitted window is retained until
        the watermark passes ``window_end + lateness_tolerance``, after
        which the window is sealed. Should match the value used by the
        upstream ``WatermarkManager`` so the two layers agree on what
        "late" means.

    The aggregator is single-threaded by design and constructed per
    consumer shard, mirroring ``WatermarkManager``.
    """

    def __init__(
        self,
        *,
        spec: WindowSpec,
        aggregation: Aggregation[Any, Any],
        lateness_tolerance_seconds: int,
    ) -> None:
        if lateness_tolerance_seconds < 0:
            raise ValueError("lateness_tolerance_seconds must be >= 0")

        self._spec = spec
        self._agg = aggregation
        self._lateness = timedelta(seconds=lateness_tolerance_seconds)

        # Per (partition_key, window_start) state. window_starts are
        # kept sorted per key in self._open_starts so we can advance
        # closure decisions without scanning the full state map.
        self._states: dict[tuple[str, datetime], _WindowState] = {}
        self._open_starts: dict[str, list[datetime]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def observe(
        self,
        *,
        partition_key: str,
        event_timestamp: datetime,
        contribution: Any,
        classification: EventClassification,
        watermark: datetime,
        trace_id: str | None = None,
    ) -> list[WindowEmission]:
        """
        Apply an event to all windows it belongs to and return any
        emissions triggered by the resulting watermark advance.

        Returns a list (possibly empty) of ``WindowEmission``s in
        chronological window-start order. Emissions include both
        first-time closures and repair re-emissions.

        ``LATE_DROPPED`` events are ignored — the upstream watermark
        manager has already decided they are too late to influence
        aggregations.

        ``trace_id`` is recorded on each window the event contributes
        to, surviving across watermark advances so that the eventual
        emission carries the most recent contributor's trace lineage.
        Pass ``None`` (the default) when not exercising trace
        propagation.
        """

        if not partition_key:
            raise ValueError("partition_key must be non-empty")
        if event_timestamp.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware")
        if watermark.tzinfo is None:
            raise ValueError("watermark must be timezone-aware")

        if classification is EventClassification.LATE_DROPPED:
            # Still let the watermark advance close any windows that the
            # caller's watermark advance has made eligible.
            return self._close_eligible_windows(partition_key, watermark)

        # Determine which window starts this event belongs to.
        starts = self._windows_containing(event_timestamp)

        emissions: list[WindowEmission] = []

        for window_start in starts:
            key = (partition_key, window_start)
            window_end = window_start + self._spec.size

            existing = self._states.get(key)
            if existing is None:
                # New window — only initialise if not already past the
                # sealing horizon. (Defence in depth; should not happen
                # if classification is correct.)
                if watermark > window_end + self._lateness:
                    continue
                state = _WindowState(
                    state=self._agg.initial(),
                    event_count=0,
                    has_emitted=False,
                )
                self._states[key] = state
                bisect.insort(
                    self._open_starts.setdefault(partition_key, []),
                    window_start,
                )
            else:
                state = existing

            # Apply the event. Window repair is the same code path —
            # combine() with the new contribution. If the window had
            # already emitted, the next emission below will be tagged
            # is_repair=True.
            state.state = self._agg.combine(state.state, contribution)
            state.event_count += 1
            state.last_contributing_trace_id = trace_id

            if classification is EventClassification.LATE_TOLERATED and state.has_emitted:
                # Re-emit immediately for repair. The window is still
                # open (not yet sealed) so its state is current.
                emissions.append(
                    WindowEmission(
                        partition_key=partition_key,
                        aggregation_name=self._agg.name,
                        window_start=window_start,
                        window_end=window_end,
                        value=self._agg.finalise(state.state),
                        event_count=state.event_count,
                        is_repair=True,
                        last_contributing_trace_id=state.last_contributing_trace_id,
                    )
                )

        # Now close any windows whose right edge is at or before the
        # watermark and which haven't yet emitted. This is what makes
        # emission watermark-driven rather than per-event.
        emissions.extend(self._close_eligible_windows(partition_key, watermark))

        # Chronological ordering across closures and repairs combined.
        emissions.sort(key=lambda e: (e.window_start, e.is_repair))
        return emissions

    def open_window_count(self, partition_key: str) -> int:
        """Diagnostic: how many windows are currently retained for a key."""
        return len(self._open_starts.get(partition_key, []))

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _windows_containing(self, event_timestamp: datetime) -> list[datetime]:
        """
        Compute the list of window starts whose ``[start, start+size)``
        interval contains ``event_timestamp``.

        For a sliding window with size=60, slide=15 and event at t=72:
        the windows are at starts 15, 30, 45, 60. We compute these in
        closed form rather than scanning state.
        """

        offset_seconds = (event_timestamp - _EPOCH).total_seconds()
        slide = self._spec.slide_seconds
        size = self._spec.size_seconds

        # The newest window containing this event has start = floor(t/slide)*slide.
        # Older windows step back by `slide` until the window no longer
        # contains the event (start + size <= t means out of window).
        newest_start_seconds = math.floor(offset_seconds / slide) * slide

        starts: list[datetime] = []
        # Number of overlapping windows is size // slide.
        for k in range(size // slide):
            start_seconds = newest_start_seconds - k * slide
            start_dt = _EPOCH + timedelta(seconds=start_seconds)
            # Strict containment: window is [start, start+size). Check
            # explicitly because for events on a slide boundary the
            # newest window already includes them.
            if start_dt <= event_timestamp < start_dt + self._spec.size:
                starts.append(start_dt)

        return sorted(starts)

    def _close_eligible_windows(
        self,
        partition_key: str,
        watermark: datetime,
    ) -> list[WindowEmission]:
        """
        Emit any windows for ``partition_key`` whose right edge is at or
        before the watermark and which have not yet emitted, then evict
        any windows past their sealing horizon.

        Returns emissions in chronological order.
        """

        open_starts = self._open_starts.get(partition_key)
        if not open_starts:
            return []

        emissions: list[WindowEmission] = []
        kept: list[datetime] = []

        for window_start in open_starts:
            window_end = window_start + self._spec.size
            key = (partition_key, window_start)
            state = self._states[key]

            if not state.has_emitted and watermark >= window_end:
                # First-time closure.
                emissions.append(
                    WindowEmission(
                        partition_key=partition_key,
                        aggregation_name=self._agg.name,
                        window_start=window_start,
                        window_end=window_end,
                        value=self._agg.finalise(state.state),
                        event_count=state.event_count,
                        is_repair=False,
                        last_contributing_trace_id=state.last_contributing_trace_id,
                    )
                )
                state.has_emitted = True

            # Sealing horizon: drop state once it can no longer be
            # touched by any tolerated late event.
            if watermark > window_end + self._lateness:
                del self._states[key]
            else:
                kept.append(window_start)

        if kept:
            self._open_starts[partition_key] = kept
        else:
            # Avoid retaining empty lists; keeps memory bounded for
            # bursty stores that go quiet.
            self._open_starts.pop(partition_key, None)

        return emissions
