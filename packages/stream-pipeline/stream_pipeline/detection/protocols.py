"""
Detector protocols.

The two protocols defined here are the load-bearing contract for
Phase 2's detection layer. Every concrete detector implements one or
both of them; the realtime pipeline registers and dispatches against
them; future Phase 5 alert routing reads detection events that flow
back from any detector that satisfies the contract.

Two shapes, one for each input type:

- ``EventDetector`` consumes raw ``TelemetryEvent``s as they arrive.
  Used by detectors that need to see every event, not just windowed
  aggregates. ``OfflineDetector`` is the canonical example: it tracks
  ``last_seen`` per (store_id, device_id) and emits when an event's
  timestamp implies a previously-seen device has gone silent past the
  configured threshold.

- ``EmissionDetector`` consumes ``WindowEmission``s from a named
  aggregator. Used by detectors that operate on windowed aggregates.
  ``OutageDetector`` and ``AnomalyDetector`` are examples: outage
  thresholds are evaluated against per-store offline counts; signal
  anomalies are evaluated against windowed signal-quality means.

The protocols are deliberately:

- not ``runtime_checkable``. Static type-checking at the
  ``register_event_detector`` / ``register_emission_detector`` call
  sites is the real safety; ``isinstance`` would be partial and
  misleading because it only checks method existence, not signature
  compatibility.

- without default implementations. Each concrete detector implements
  every protocol member explicitly. More boilerplate, more legibility:
  a reader of ``OfflineDetector`` sees its ``name`` and ``observe_event``
  in the class body rather than having to know "look in the protocol
  for default behaviour".

- function-shaped. No ``flush()``, ``close()``, or ``reset()``
  lifecycle methods. Detectors are pure observers of their input
  stream; lifecycle concerns live in the pipeline layer that owns
  them.

Replay determinism: every detector must derive ``DetectionEvent``
field values from its inputs and its own internal state, never from
``datetime.now()`` or any other wall-clock source. ``EventDetector``s
typically set ``detected_at = event.event_timestamp``; ``EmissionDetector``s
typically set ``detected_at = emission.window_end``. Detection IDs
must be derived from inputs (e.g. ``uuid5`` over a stable seed) rather
than allocated freshly so replays produce identical outputs. Concrete
detectors are responsible for honouring this contract; the protocol
declares only the shape, not the determinism guarantees.
"""

from __future__ import annotations

from typing import ClassVar, Protocol

from event_schema_contracts.detection import DetectionEvent

from stream_pipeline.streaming.event_protocol import TelemetryEvent
from stream_pipeline.streaming.window_aggregator import WindowEmission


class EventDetector(Protocol):
    """
    Detector that consumes raw telemetry events.

    Implementations observe every event dispatched through the
    ``RealtimePipeline``'s router and may emit zero or more
    ``DetectionEvent``s in response. The pipeline collects emitted
    detections into the ``ProcessingResult.detections`` list returned
    to the caller of ``process()``.

    The ``name`` attribute identifies the detector in pipeline log
    lines and in test assertions. Conventionally it matches the
    detector's class name (e.g. ``"OfflineDetector"``).

    ``observe_event`` is the only method called per-event. It must:

    - return a (possibly empty) list of ``DetectionEvent``s
    - be deterministic given identical input sequences (no wall-clock
      reads, no unseeded random)
    - not raise on well-formed input — internal contradictions should
      be expressed as detection events (e.g. with severity CRITICAL
      and a diagnostic ``threshold_breached`` message) rather than
      exceptions, so a single malformed observation does not poison
      the stream

    Failure isolation at the pipeline level still applies: an
    exception escaping ``observe_event`` is caught and logged by the
    pipeline, but the detector should not rely on that as its primary
    error-handling strategy.
    """

    name: ClassVar[str]

    def observe_event(self, event: TelemetryEvent) -> list[DetectionEvent]: ...


class EmissionDetector(Protocol):
    """
    Detector that consumes window emissions from a named aggregator.

    Implementations subscribe to the emissions of a single named
    aggregator registered on the ``RealtimePipeline``. The pipeline
    dispatches each emission to every emission detector whose
    ``aggregation_name`` matches the emission's source.

    The ``aggregation_name`` is the routing key; concrete detectors
    declare it as a ``ClassVar[str]`` to keep registration declarative
    rather than per-instance.

    ``observe_emission`` follows the same return-list / determinism /
    failure-isolation contract as ``EventDetector.observe_event``.
    """

    name: ClassVar[str]
    aggregation_name: ClassVar[str]

    def observe_emission(self, emission: WindowEmission) -> list[DetectionEvent]: ...
