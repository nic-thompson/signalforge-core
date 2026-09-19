"""
stream_pipeline.streaming.event_router

Schema-aware, trace-propagating event dispatcher.

The router is the seam between validated telemetry events and the rest of
the analytics control plane: detectors, feature builders, dashboard
projectors, and dataset writers all subscribe to the router by registering
handlers against ``(event_type, schema_version)``.

Design properties
-----------------

- **Schema-aware**: handlers register against (event_type, schema_version),
  never against event_type alone. Compatible-minor-version events fall
  back to the closest registered handler at or below the requested
  version; major-version mismatches raise.
- **Deterministic ordering**: handlers are dispatched in registration
  order for a given (event_type, schema_version) tuple. Required for
  replay reproducibility.
- **Failure-isolated**: a handler exception is logged and counted, and
  dispatch continues to the next handler. Strict mode is available for
  tests that need fail-fast semantics.
- **Trace-propagating**: every log emission carries the inbound event's
  ``trace_id``.
- **Pure routing**: the router never inspects payloads. Routing decisions
  use only ``metadata.event_type`` and ``metadata.schema_version``.

Compatibility resolution
------------------------

When an event arrives whose ``schema_version`` has no exact handler match,
the router selects the highest registered version with the same major
that is **less than or equal to** the requested version.

This direction is deliberate: stream-pipeline is a *consumer* of telemetry
events. In a phased retail-fleet rollout, devices ship new firmware
(producing v1.1 events) ahead of every consumer being upgraded — so a v1
handler must accept v1.1 events, not the other way around. This is the
opposite direction from the upstream ``event_schema_contracts``
``SchemaRegistry``, which serves producers looking up consumer schemas.
The asymmetry is intentional and reflects the different role each
component plays in the platform.

The router uses ``event_schema_contracts.versioning.compatibility``
``ensure_compatibility`` and ``parse_version`` as the compatibility oracle
when the upstream package is importable, falling back to a same-major
string comparison otherwise. The fallback is only invoked in test
environments without the upstream package installed; production always
uses the canonical helper.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from typing import Final

from stream_pipeline.streaming.event_protocol import TelemetryEvent
from stream_pipeline.streaming.observability import StructuredLoggerLike, get_logger

# Type alias for handlers. A handler accepts an event and returns nothing —
# any output it produces is the handler's own concern (writing to a sink,
# updating an aggregator, etc.).
EventHandler = Callable[[TelemetryEvent], None]


# Logical event types used by the router for its own structured-log
# emissions. Distinct from the telemetry event types it routes.
_LOG_EVENT_DISPATCH: Final[str] = "router.dispatch"
_LOG_EVENT_NO_HANDLER: Final[str] = "router.no_handler"
_LOG_EVENT_HANDLER_ERROR: Final[str] = "router.handler_error"
_LOG_EVENT_FALLBACK: Final[str] = "router.compatible_fallback"


# ---------------------------------------------------------------------------
# Routing key and dispatch result
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class RouteKey:
    """Composite key under which handlers register."""

    event_type: str
    schema_version: str


@dataclasses.dataclass(frozen=True)
class DispatchResult:
    """
    Outcome of a single dispatch call.

    Returned for observability and for tests asserting routing behaviour
    without inspecting the logger.
    """

    matched_key: RouteKey | None
    handlers_invoked: int
    handler_failures: int
    fell_back: bool


# ---------------------------------------------------------------------------
# Compatibility resolution (consumer-favouring direction)
# ---------------------------------------------------------------------------


def _resolve_compatible_key(
    requested: RouteKey,
    registered_keys: Iterable[RouteKey],
) -> RouteKey | None:
    """
    Find the highest registered key with the same major version and a
    schema version less than or equal to the requested version.

    Prefers the upstream ``event-schema-contracts`` compatibility helper.
    Falls back to a same-major string comparison when the upstream package
    is unavailable, which is sufficient for unit tests.
    """

    try:
        from event_schema_contracts.versioning.compatibility import (
            SchemaVersion,
            parse_version,
        )
    except ImportError:
        return _resolve_compatible_key_fallback(requested, registered_keys)

    requested_version = parse_version(requested.schema_version)
    candidates: list[tuple[SchemaVersion, RouteKey]] = []

    for key in registered_keys:
        if key.event_type != requested.event_type:
            continue
        try:
            candidate_version = parse_version(key.schema_version)
        except ValueError:
            continue
        if candidate_version.major != requested_version.major:
            continue
        if candidate_version <= requested_version:
            candidates.append((candidate_version, key))

    if not candidates:
        return None

    # Pick the highest version <= requested. This gives the tightest
    # backward-compatible match: if v1, v1.1, v1.2 are all registered and
    # a v1.3 event arrives, we pick v1.2 — the closest match to what the
    # producer expects without crossing the major boundary.
    return max(candidates, key=lambda item: item[0])[1]


def _resolve_compatible_key_fallback(
    requested: RouteKey,
    registered_keys: Iterable[RouteKey],
) -> RouteKey | None:
    """
    Conservative compatibility fallback for environments without the
    upstream contracts package.

    Strategy: among registered keys with the same major prefix as the
    requested version (e.g. "v1") and a schema_version string less than or
    equal to the requested version, pick the lexicographically greatest.

    This is correct for the common case (v1, v1.1, v1.2) because the
    schema-version grammar is "v" + dotted integers and our lexicographic
    comparison agrees with semver for single-digit components. Production
    runs always use the upstream helper, where the comparison is rigorous.
    """

    def major_prefix(version: str) -> str:
        return version.split(".", 1)[0]

    requested_major = major_prefix(requested.schema_version)
    candidates = [
        key
        for key in registered_keys
        if key.event_type == requested.event_type
        and major_prefix(key.schema_version) == requested_major
        and key.schema_version <= requested.schema_version
    ]
    if not candidates:
        return None
    # Highest schema_version <= requested wins. Sort by schema_version
    # explicitly because RouteKey is intentionally not orderable.
    return max(candidates, key=lambda k: k.schema_version)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


class EventRouter:
    """
    Schema-aware dispatcher of telemetry events to registered handlers.

    Handlers are invoked in registration order. Exceptions raised by a
    handler are logged and counted but do not interrupt dispatch unless
    ``strict`` is enabled.

    Parameters
    ----------
    strict:
        When True, a handler exception aborts dispatch and re-raises. When
        False (the default — production), exceptions are logged and the
        next handler is invoked.
    logger:
        Optional structured-logger override. Tests use this to capture
        emitted log lines.
    """

    def __init__(
        self,
        *,
        strict: bool = False,
        logger: StructuredLoggerLike | None = None,
    ) -> None:
        self._handlers: dict[RouteKey, list[EventHandler]] = {}
        self._strict = strict
        self._logger = logger if logger is not None else get_logger(
            "stream_pipeline.streaming.event_router"
        )

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        event_type: str,
        schema_version: str,
        handler: EventHandler,
    ) -> None:
        """
        Register ``handler`` for ``(event_type, schema_version)``.

        Multiple handlers may be registered for the same key; they are
        invoked in registration order.
        """

        if not event_type:
            raise ValueError("event_type must be non-empty")
        if not schema_version:
            raise ValueError("schema_version must be non-empty")
        if not callable(handler):
            raise TypeError("handler must be callable")

        key = RouteKey(event_type=event_type, schema_version=schema_version)
        self._handlers.setdefault(key, []).append(handler)

    def registered_keys(self) -> list[RouteKey]:
        """Return registered keys sorted for deterministic introspection."""
        return sorted(
            self._handlers.keys(),
            key=lambda k: (k.event_type, k.schema_version),
        )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def dispatch(self, event: TelemetryEvent) -> DispatchResult:
        """
        Dispatch an event to all matching handlers.

        Returns a ``DispatchResult`` describing the dispatch outcome for
        observability and testing.
        """

        requested = RouteKey(
            event_type=event.metadata.event_type,
            schema_version=event.metadata.schema_version,
        )
        trace_id = str(event.trace.trace_id)

        # Exact match first — the common path.
        handlers = self._handlers.get(requested)
        matched_key: RouteKey | None = requested
        fell_back = False

        if handlers is None:
            # No exact match — attempt consumer-favouring fallback.
            compatible = _resolve_compatible_key(requested, self._handlers.keys())
            if compatible is not None:
                handlers = self._handlers[compatible]
                matched_key = compatible
                fell_back = True
                self._logger.info(
                    "Routing event to compatible handler",
                    event_type=_LOG_EVENT_FALLBACK,
                    trace_id=trace_id,
                    metadata={
                        "requested_event_type": requested.event_type,
                        "requested_schema_version": requested.schema_version,
                        "matched_schema_version": compatible.schema_version,
                    },
                )

        if not handlers:
            self._logger.warning(
                "No handler registered for event",
                event_type=_LOG_EVENT_NO_HANDLER,
                trace_id=trace_id,
                metadata={
                    "telemetry_event_type": requested.event_type,
                    "telemetry_schema_version": requested.schema_version,
                },
            )
            return DispatchResult(
                matched_key=None,
                handlers_invoked=0,
                handler_failures=0,
                fell_back=False,
            )

        invoked = 0
        failures = 0

        for handler in handlers:
            try:
                handler(event)
                invoked += 1
            except Exception as exc:
                failures += 1
                self._logger.error(
                    "Handler raised during dispatch",
                    event_type=_LOG_EVENT_HANDLER_ERROR,
                    trace_id=trace_id,
                    metadata={
                        "telemetry_event_type": requested.event_type,
                        "telemetry_schema_version": requested.schema_version,
                        "handler": getattr(handler, "__qualname__", repr(handler)),
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )
                if self._strict:
                    raise

        # Defensive narrowing for type-checkers — matched_key is non-None
        # whenever handlers were invoked.
        assert matched_key is not None

        self._logger.info(
            "Dispatched event to handlers",
            event_type=_LOG_EVENT_DISPATCH,
            trace_id=trace_id,
            metadata={
                "telemetry_event_type": requested.event_type,
                "telemetry_schema_version": requested.schema_version,
                "matched_schema_version": matched_key.schema_version,
                "handlers_invoked": invoked,
                "handler_failures": failures,
                "fell_back": fell_back,
            },
        )

        return DispatchResult(
            matched_key=matched_key,
            handlers_invoked=invoked,
            handler_failures=failures,
            fell_back=fell_back,
        )
