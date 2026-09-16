from datetime import datetime, timezone
from typing import Iterator, List, Tuple

from telemetry_parser.observability.parser_observer import ParserObserver
from telemetry_parser.stream.observation import (
    TimestampedChunk,
    TimestampedMessage,
)


class MessageDecoder:
    """
    Incremental SIP header-boundary detector.

    Supports:
    - multi-message streams
    - partial buffering
    - replay-safe deterministic framing

    Framing does not align with packet boundaries: one chunk may contain
    several messages, and one message may span several chunks. So the
    decoder cannot simply carry "the current timestamp" — after a message
    is consumed, the next message's first byte may come from a chunk that
    was already partly used.

    It therefore keeps marks: the buffer offset at which each chunk's
    bytes begin, paired with that chunk's observation time. A message
    starting at offset zero takes the timestamp of the mark covering
    offset zero, which is the packet that supplied its first byte.
    """

    HEADER_TERMINATOR = b"\r\n\r\n"

    def __init__(
        self,
        observer: ParserObserver | None = None,
    ) -> None:

        self.buffer: bytearray = bytearray()
        self.observer = observer

        # (offset into self.buffer, observation time of the chunk starting there)
        # Kept sorted ascending. The first mark is always at offset 0 while
        # the buffer is non-empty, so it covers the next message's first byte.
        self._marks: List[Tuple[int, datetime]] = []

    def feed(self, chunk: TimestampedChunk) -> Iterator[TimestampedMessage]:
        """
        Accepts reconstructed TCP payload chunks
        and yields complete SIP header blocks.
        """

        if chunk.data:
            self._marks.append((len(self.buffer), chunk.timestamp))
            self.buffer.extend(chunk.data)

        terminator_len = len(self.HEADER_TERMINATOR)

        while True:
            boundary = self.buffer.find(self.HEADER_TERMINATOR)

            if boundary == -1:
                return

            message_end = boundary + terminator_len

            message = bytes(self.buffer[:message_end])
            observed_at = self._first_byte_timestamp()

            if self.observer:
                self.observer.on_message_reconstructed(
                    len(message),
                    datetime.now(timezone.utc),
                )

            del self.buffer[:message_end]
            self._advance_marks(message_end)

            yield TimestampedMessage(timestamp=observed_at, data=message)

    def flush(self) -> TimestampedMessage | None:
        """
        Returns remaining buffered data (if any).
        Used during session teardown.

        The remainder has no header terminator, so it is by definition an
        incomplete SIP message.
        """

        if not self.buffer:
            return None

        remainder = TimestampedMessage(
            timestamp=self._first_byte_timestamp(),
            data=bytes(self.buffer),
        )

        self.buffer.clear()
        self._marks.clear()

        return remainder

    def _first_byte_timestamp(self) -> datetime:
        """
        Observation time of the packet that supplied byte zero of the
        buffer. Marks are maintained so that the first one always covers
        offset zero while the buffer holds data.
        """

        if not self._marks:
            raise ValueError(
                "no observation timestamp for buffered data; "
                "feed() must supply a TimestampedChunk before framing"
            )

        return self._marks[0][1]

    def _advance_marks(self, consumed: int) -> None:
        """
        Rebases marks after ``consumed`` bytes are removed from the front.

        A chunk that is now only partly consumed keeps covering offset
        zero, so marks that move to or below zero collapse into a single
        mark at zero — the last of them, since that is the chunk the
        remaining bytes came from.
        """

        if not self.buffer:
            self._marks.clear()
            return

        rebased: List[Tuple[int, datetime]] = []

        for offset, timestamp in self._marks:
            moved = offset - consumed

            if moved <= 0:
                rebased = [(0, timestamp)]
            else:
                rebased.append((moved, timestamp))

        self._marks = rebased
