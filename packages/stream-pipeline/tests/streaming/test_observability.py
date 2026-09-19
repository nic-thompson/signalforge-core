"""
Tests for stream_pipeline.streaming.observability.

Covers:

- get_logger returns a usable logger regardless of upstream availability
- the recording logger captures records faithfully for downstream tests
- log calls survive missing optional kwargs

Note: structural conformance of RecordingLogger to StructuredLoggerLike is
enforced statically by mypy and at every call site that accepts a logger
parameter. We do not assert it at runtime because @runtime_checkable
protocols only verify method *names*, not signatures — so the check would
add noise without adding real safety.
"""

from __future__ import annotations

import unittest

from stream_pipeline.streaming.observability import get_logger
from tests._fixtures.events import RecordingLogger


class GetLoggerTest(unittest.TestCase):
    def test_get_logger_returns_usable_logger(self):
        logger = get_logger("stream_pipeline.test")
        # Should not raise, regardless of which backend is selected.
        logger.info("startup", event_type="log.info", trace_id="abc")
        logger.warning("warn", event_type="log.warn", trace_id="abc")
        logger.error("err", event_type="log.err", trace_id="abc")

    def test_get_logger_accepts_minimal_kwargs(self):
        logger = get_logger("stream_pipeline.test")
        # Calling with only the message should also work.
        logger.info("hello")


class RecordingLoggerTest(unittest.TestCase):
    def test_recording_logger_captures_records(self):
        logger = RecordingLogger()
        logger.info(
            "dispatched",
            event_type="router.dispatch",
            metadata={"handlers": 3},
            trace_id="trace-123",
        )

        self.assertEqual(len(logger.records), 1)
        record = logger.records[0]
        self.assertEqual(record.level, "INFO")
        self.assertEqual(record.event_type, "router.dispatch")
        self.assertEqual(record.metadata, {"handlers": 3})
        self.assertEqual(record.trace_id, "trace-123")

    def test_by_event_type_filters_correctly(self):
        logger = RecordingLogger()
        logger.info("a", event_type="router.dispatch")
        logger.warning("b", event_type="router.no_handler")
        logger.info("c", event_type="router.dispatch")

        dispatched = logger.by_event_type("router.dispatch")
        self.assertEqual(len(dispatched), 2)
        self.assertEqual(dispatched[0].message, "a")
        self.assertEqual(dispatched[1].message, "c")

    def test_recording_logger_handles_missing_metadata(self):
        # The streaming components occasionally emit logs without metadata
        # — the recording logger must default to an empty dict, not None,
        # so test assertions can read .metadata[...] uniformly.
        logger = RecordingLogger()
        logger.info("bare", event_type="router.dispatch")

        self.assertEqual(logger.records[0].metadata, {})


if __name__ == "__main__":
    unittest.main()
