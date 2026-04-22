from typing import Iterator, Iterable, Callable, Mapping, Any

from telemetry_parser.output.structured_event import StructuredEvent


class EventEmitter:
    """
    Emits structured telemetry events in streaming-safe format.

    Supports:
    - iterator-based pipelines
    - replay workflows
    - analytics ingestion
    - observability hooks via on_emit callback

    Stateless and thread-safe unless the callback is not.
    """

    def __init__(
        self,
        on_emit: Callable[[StructuredEvent], None] | None = None,
    ) -> None:
        """
        Optional callback invoked once per emitted event.
        """

        self.on_emit = on_emit

    def emit(
        self,
        event: StructuredEvent,
    ) -> StructuredEvent:
        """
        Emits a single structured event.
        """

        if self.on_emit is not None:
            self.on_emit(event)

        return event

    def emit_many(
        self,
        events: Iterable[StructuredEvent],
    ) -> Iterator[StructuredEvent]:
        """
        Emits multiple structured events lazily.
        """

        for event in events:
            yield self.emit(event)

    def emit_json(
        self,
        event: StructuredEvent,
    ) -> Mapping[str, Any]:
        """
        Emits JSON-safe event representation.
        """

        return self.emit(event).to_json_safe()

    def emit_many_json(
        self,
        events: Iterable[StructuredEvent],
    ) -> Iterator[Mapping[str, Any]]:
        """
        Emits multiple JSON-safe events lazily.
        """

        for event in events:
            yield self.emit_json(event)