from typing import Callable

from telemetry_parser.output.structured_event import StructuredEvent


class EventEmitter:
    """
    Emits structured telemetry events in streaming-safe format.

    Supports:
    - iterator-based pipelines
    - replay workflows
    - analytics ingestion
    - dataset regeneration
    - structured logging integration (future)
    """

    def __init__(
        self,
        preserve_event_ids: bool = False,
        id_provider: Callable[[], str] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        preserve_event_ids:
            Ensures event identifiers remain stable during replay or dataset regeneration.

        id_provider:
            Optional deterministic ID provider used when generating replacement event IDs.
        """

        self.preserve_event_ids = preserve_event_ids
        self.id_provider = id_provider

    def emit(
        self,
        event: StructuredEvent,
    ) -> StructuredEvent:
        """
        Emits a structured telemetry event.

        Behaviour depends on replay configuration:

        - If preserve_event_ids=True → event IDs remain unchanged
        - If preserve_event_ids=False and id_provider supplied → deterministic IDs injected
        - Otherwise → event emitted unchanged
        """

        if not self.preserve_event_ids and self.id_provider is not None:
            event.event_id = self.id_provider()

        return event