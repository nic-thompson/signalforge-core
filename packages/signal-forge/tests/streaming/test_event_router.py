"""
Tests for signal_forge.streaming.event_router.

Covers:

- exact-match dispatch
- multi-handler fan-out preserving registration order
- consumer-favouring compatible-version fallback (highest <= requested)
- major-version mismatch does not fall back
- exact match wins over fallback
- handler failure isolation (default mode)
- handler failure propagation (strict mode)
- trace_id propagation onto emitted log lines
- no-handler case emits warning and returns null result
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime

from signal_forge.streaming.event_router import EventRouter, RouteKey
from tests._fixtures.events import FakeEvent, RecordingLogger


def _make_event(event_type: str, schema_version: str) -> FakeEvent:
    return FakeEvent(
        event_type=event_type,
        schema_version=schema_version,
        event_timestamp=datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC),
    )


class EventRouterRegistrationTest(unittest.TestCase):
    def test_register_rejects_empty_event_type(self):
        router = EventRouter(logger=RecordingLogger())
        with self.assertRaises(ValueError):
            router.register("", "v1", lambda e: None)

    def test_register_rejects_empty_schema_version(self):
        router = EventRouter(logger=RecordingLogger())
        with self.assertRaises(ValueError):
            router.register("device.registration", "", lambda e: None)

    def test_register_rejects_non_callable_handler(self):
        router = EventRouter(logger=RecordingLogger())
        with self.assertRaises(TypeError):
            router.register("device.registration", "v1", "not-callable")  # type: ignore[arg-type]

    def test_registered_keys_are_sorted(self):
        router = EventRouter(logger=RecordingLogger())
        router.register("network.connection", "v1", lambda e: None)
        router.register("device.registration", "v1", lambda e: None)
        router.register("device.registration", "v2", lambda e: None)

        keys = router.registered_keys()
        self.assertEqual(
            keys,
            [
                RouteKey("device.registration", "v1"),
                RouteKey("device.registration", "v2"),
                RouteKey("network.connection", "v1"),
            ],
        )


class EventRouterDispatchTest(unittest.TestCase):
    def test_dispatch_invokes_exact_match_handler(self):
        router = EventRouter(logger=RecordingLogger())
        seen: list[str] = []
        router.register(
            "device.registration", "v1", lambda e: seen.append(str(e.event_id))
        )

        event = _make_event("device.registration", "v1")
        result = router.dispatch(event)

        self.assertEqual(result.handlers_invoked, 1)
        self.assertEqual(result.handler_failures, 0)
        self.assertFalse(result.fell_back)
        self.assertEqual(result.matched_key, RouteKey("device.registration", "v1"))
        self.assertEqual(seen, [str(event.event_id)])

    def test_dispatch_preserves_handler_registration_order(self):
        # Determinism is required for replay. Handlers MUST execute in the
        # order they were registered, not in dict iteration order.
        router = EventRouter(logger=RecordingLogger())
        order: list[int] = []

        for i in range(5):
            router.register(
                "device.registration",
                "v1",
                lambda e, i=i: order.append(i),
            )

        router.dispatch(_make_event("device.registration", "v1"))
        self.assertEqual(order, [0, 1, 2, 3, 4])

    def test_dispatch_to_multiple_handlers_counts_invocations(self):
        router = EventRouter(logger=RecordingLogger())
        router.register("device.registration", "v1", lambda e: None)
        router.register("device.registration", "v1", lambda e: None)
        router.register("device.registration", "v1", lambda e: None)

        result = router.dispatch(_make_event("device.registration", "v1"))
        self.assertEqual(result.handlers_invoked, 3)

    def test_dispatch_with_no_handler_emits_warning_and_returns_null_result(self):
        logger = RecordingLogger()
        router = EventRouter(logger=logger)

        result = router.dispatch(_make_event("device.registration", "v1"))

        self.assertIsNone(result.matched_key)
        self.assertEqual(result.handlers_invoked, 0)
        warnings = logger.by_event_type("router.no_handler")
        self.assertEqual(len(warnings), 1)
        self.assertEqual(warnings[0].level, "WARNING")


class EventRouterFallbackTest(unittest.TestCase):
    def test_dispatch_falls_back_to_older_compatible_handler(self):
        # Handler registered at v1; event arrives at v1.1 — same major,
        # newer minor produced by an upgraded fleet device. The v1 handler
        # MUST receive the event (consumer-favouring fallback).
        logger = RecordingLogger()
        router = EventRouter(logger=logger)
        seen: list[str] = []
        router.register("device.registration", "v1", lambda e: seen.append("v1"))

        result = router.dispatch(_make_event("device.registration", "v1.1"))

        self.assertEqual(seen, ["v1"])
        self.assertTrue(result.fell_back)
        self.assertEqual(result.matched_key, RouteKey("device.registration", "v1"))
        fallbacks = logger.by_event_type("router.compatible_fallback")
        self.assertEqual(len(fallbacks), 1)
        self.assertEqual(
            fallbacks[0].metadata["requested_schema_version"], "v1.1"
        )
        self.assertEqual(
            fallbacks[0].metadata["matched_schema_version"], "v1"
        )

    def test_fallback_picks_highest_version_at_or_below_requested(self):
        # Three handlers: v1, v1.1, v1.2. A v1.3 event arrives. The v1.2
        # handler must receive it — the tightest backward-compatible match.
        router = EventRouter(logger=RecordingLogger())
        seen: list[str] = []
        router.register("device.registration", "v1", lambda e: seen.append("v1"))
        router.register("device.registration", "v1.1", lambda e: seen.append("v1.1"))
        router.register("device.registration", "v1.2", lambda e: seen.append("v1.2"))

        result = router.dispatch(_make_event("device.registration", "v1.3"))

        self.assertEqual(seen, ["v1.2"])
        self.assertEqual(
            result.matched_key, RouteKey("device.registration", "v1.2")
        )

    def test_dispatch_does_not_fall_back_across_majors(self):
        # v2 event has no v2 handler; only v1 handler exists. The router
        # MUST NOT cross the major-version boundary — that's a deliberate
        # breaking-change boundary.
        logger = RecordingLogger()
        router = EventRouter(logger=logger)
        seen: list[str] = []
        router.register("device.registration", "v1", lambda e: seen.append("v1"))

        result = router.dispatch(_make_event("device.registration", "v2"))

        self.assertEqual(seen, [])
        self.assertIsNone(result.matched_key)
        self.assertEqual(len(logger.by_event_type("router.no_handler")), 1)

    def test_does_not_fall_back_to_newer_version(self):
        # Only a v1.2 handler is registered; a v1 event arrives. We must
        # NOT dispatch — the v1.2 handler may rely on fields the v1 event
        # does not carry.
        router = EventRouter(logger=RecordingLogger())
        seen: list[str] = []
        router.register("device.registration", "v1.2", lambda e: seen.append("v1.2"))

        result = router.dispatch(_make_event("device.registration", "v1"))

        self.assertEqual(seen, [])
        self.assertIsNone(result.matched_key)

    def test_exact_match_takes_precedence_over_fallback(self):
        logger = RecordingLogger()
        router = EventRouter(logger=logger)
        seen: list[str] = []
        router.register("device.registration", "v1", lambda e: seen.append("v1"))
        router.register("device.registration", "v1.1", lambda e: seen.append("v1.1"))

        router.dispatch(_make_event("device.registration", "v1.1"))

        # v1.1 event hits only the v1.1 handler, not v1.
        self.assertEqual(seen, ["v1.1"])
        # No fallback log emitted for an exact match.
        self.assertEqual(logger.by_event_type("router.compatible_fallback"), [])


class EventRouterFailureIsolationTest(unittest.TestCase):
    def test_handler_exception_is_isolated_in_default_mode(self):
        # Default behaviour: handler failures are logged and counted, the
        # next handler still runs. This protects the stream from one bad
        # subscriber poisoning every event.
        logger = RecordingLogger()
        router = EventRouter(logger=logger)

        invoked: list[str] = []

        def bad(_):
            raise RuntimeError("boom")

        def good(_):
            invoked.append("good")

        router.register("device.registration", "v1", bad)
        router.register("device.registration", "v1", good)

        result = router.dispatch(_make_event("device.registration", "v1"))

        self.assertEqual(result.handler_failures, 1)
        self.assertEqual(result.handlers_invoked, 1)
        self.assertEqual(invoked, ["good"])

        errors = logger.by_event_type("router.handler_error")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].metadata["exception_type"], "RuntimeError")
        self.assertEqual(errors[0].metadata["exception_message"], "boom")

    def test_strict_mode_propagates_handler_exceptions(self):
        # Strict mode is intended for tests and replay-validation runs that
        # want fail-fast behaviour. Handler exceptions must propagate.
        router = EventRouter(strict=True, logger=RecordingLogger())

        def bad(_):
            raise RuntimeError("boom")

        router.register("device.registration", "v1", bad)

        with self.assertRaises(RuntimeError):
            router.dispatch(_make_event("device.registration", "v1"))


class EventRouterTracePropagationTest(unittest.TestCase):
    def test_trace_id_appears_on_dispatch_log_line(self):
        logger = RecordingLogger()
        router = EventRouter(logger=logger)
        router.register("device.registration", "v1", lambda e: None)

        event = _make_event("device.registration", "v1")
        router.dispatch(event)

        dispatched = logger.by_event_type("router.dispatch")
        self.assertEqual(len(dispatched), 1)
        self.assertEqual(dispatched[0].trace_id, str(event.trace.trace_id))

    def test_trace_id_appears_on_no_handler_warning(self):
        logger = RecordingLogger()
        router = EventRouter(logger=logger)

        event = _make_event("device.registration", "v1")
        router.dispatch(event)

        warnings = logger.by_event_type("router.no_handler")
        self.assertEqual(warnings[0].trace_id, str(event.trace.trace_id))

    def test_trace_id_appears_on_handler_error(self):
        logger = RecordingLogger()
        router = EventRouter(logger=logger)
        router.register("device.registration", "v1", lambda e: 1 / 0)

        event = _make_event("device.registration", "v1")
        router.dispatch(event)

        errors = logger.by_event_type("router.handler_error")
        self.assertEqual(errors[0].trace_id, str(event.trace.trace_id))


if __name__ == "__main__":
    unittest.main()
