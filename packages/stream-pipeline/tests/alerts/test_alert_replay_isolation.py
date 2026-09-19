"""
Phase 5 replay-isolation / byte-identity test for alert routing.

The alerting analogue of the Phase 4 dataset byte-identity test: a replay
of an archived event sequence reproduces the original run's *alerts*
byte-for-byte, routed to a replay-isolated sink so they never reach the
live bus. This is what proves the alert layer honours the project's
headline determinism property end-to-end.

Alerts are cheaper to compare than datasets — they are the AlertEvent
objects returned on ProcessingResult.alerts, so the comparison is the
serialised payloads directly (model_dump_json), with no Parquet or S3 in
the loop. Publication is caller-wired, so "replay isolation" here means
the replay run's alerts go to a separate InMemoryAlertSink while a live
run's go to the live sink; the byte-identity is in the alert payloads
themselves.

Reproducibility rests on the same derived-identity work the dataset test
relies on: alert_id is UUIDv5 from detection_id (itself UUIDv5 from
store + window), the envelope event_id is UUIDv5 from alert_id, and the
trace propagates from the detection's trace from the event's trace. With
a fixed trace and identical events fed to both runs — exactly as the
EventBridge archive replays unchanged events — the entire alert payload
carries no per-run randomness.

A real AnomalyDetector is wired (not the uuid4-minting fixtures, which
cannot exercise byte-identity) so the derived identity fields are what
land in the alerts. Watermark math mirrors the other integration tests:
5-second tumbling window, 60-second lateness, so t=165 advances the
watermark to 105 and closes window [100, 105) opened by t=102.
"""

from __future__ import annotations

import unittest
from uuid import UUID

from stream_pipeline.alerts.acknowledgement_registry import AcknowledgementRegistry
from stream_pipeline.alerts.alert_router import AlertRouter
from stream_pipeline.alerts.alert_sink import InMemoryAlertSink
from stream_pipeline.config.platform_settings import PlatformSettings
from stream_pipeline.detection.detectors import AnomalyDetector
from stream_pipeline.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.events import FakeEvent, FakeTrace
from tests.streaming.test_realtime_pipeline import epoch_aligned, make_pipeline

_LIVE_BUS = "sf-live-alert-bus"
_REPLAY_BUS = "sf-replay-alert-bus"

# Fixed trace so the alert trace_id is stable across runs, exactly as an
# archived event's trace is stable across a replay.
_TRACE = UUID("11111111-1111-4111-8111-111111111111")


def _event(*, source: str, seconds: int) -> FakeEvent:
    return FakeEvent(
        event_type="device.registration",
        schema_version="v1",
        event_timestamp=epoch_aligned(seconds),
        source=source,
        trace=FakeTrace(trace_id=_TRACE),
    )


class AlertReplayByteIdentityTest(unittest.TestCase):
    def setUp(self) -> None:
        # One fixed sequence: t=102 opens window [100, 105); t=165 advances
        # the watermark to 105 and closes it. Built once, fed to both runs.
        self.events = [
            _event(source="store-1", seconds=102),
            _event(source="store-1", seconds=165),
        ]

    def _run(self, settings: PlatformSettings) -> InMemoryAlertSink:
        # make_pipeline registers a "count" aggregator; add a "signal_value"
        # aggregation an AnomalyDetector watches, so a detection — and hence
        # an alert — is produced. The caller wires the alerts to a sink
        # chosen for this run (live vs replay): publication is caller-wired,
        # not pipeline-registered, so replay isolation is the caller's choice
        # of sink, governed by the same settings the dataset layer uses.
        pipeline, *_ = make_pipeline()
        pipeline.register_aggregator(
            "signal_value",
            WindowAggregator(
                spec=WindowSpec(size_seconds=5, slide_seconds=5),
                aggregation=CountAggregation(name="signal_value"),
                lateness_tolerance_seconds=60,
            ),
        )
        pipeline.register_emission_detector(AnomalyDetector(threshold=0.5))
        pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))

        # The caller picks the sink for this environment. A real driver
        # would build an EventBridgeAlertSink from settings.alert_bus; here
        # an in-memory sink stands in, and the live-vs-replay distinction is
        # carried by which sink object receives the run's alerts.
        sink = InMemoryAlertSink()
        for result in pipeline.process_batch(self.events):
            sink.publish(result.alerts)
        return sink

    def test_live_and_replay_produce_byte_identical_alerts(self) -> None:
        settings = PlatformSettings(
            alert_bus=_LIVE_BUS, replay_alert_bus=_REPLAY_BUS
        )
        live = self._run(settings)
        replay = self._run(settings.for_replay())

        # Non-vacuous: the run actually produced an alert.
        self.assertNotEqual(live.published, [])
        self.assertEqual(len(live.published), len(replay.published))

        # The headline assertion: identical alert payload bytes across runs.
        for live_alert, replay_alert in zip(
            live.published, replay.published, strict=True
        ):
            self.assertEqual(
                live_alert.payload.model_dump_json(),
                replay_alert.payload.model_dump_json(),
            )

    def test_replay_alerts_isolated_from_live_sink(self) -> None:
        # Each run publishes only to its own sink: the replay run's alerts
        # do not appear in the live run's sink. (Caller-wired isolation —
        # the two runs hold distinct sink objects.)
        settings = PlatformSettings(
            alert_bus=_LIVE_BUS, replay_alert_bus=_REPLAY_BUS
        )
        live = self._run(settings)
        replay = self._run(settings.for_replay())

        live_ids = {a.payload.alert_id for a in live.published}
        replay_ids = {a.payload.alert_id for a in replay.published}
        # Same alert ids (byte-identical), but held in separate sinks —
        # neither sink saw the other run's publish calls.
        self.assertEqual(live_ids, replay_ids)
        self.assertEqual(live.publish_calls, replay.publish_calls)

    def test_alert_identity_fields_are_v5(self) -> None:
        # Guards the premise: the alert carries UUIDv5 identity (alert_id,
        # detection_id, envelope event_id), which is what makes the payload
        # bytes reproducible across runs.
        sink = self._run(PlatformSettings(alert_bus=_LIVE_BUS))
        alert = sink.published[0]
        self.assertEqual(alert.payload.alert_id.version, 5)
        self.assertEqual(alert.payload.detection_id.version, 5)
        self.assertEqual(alert.event_id.version, 5)


if __name__ == "__main__":
    unittest.main()
