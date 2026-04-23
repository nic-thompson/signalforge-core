import datetime
from typing import Iterator
from telemetry_parser.observability.parser_observer import ParserObserver

class MessageDecoder:
    """
    Incremental SIP header-boundary detector.

    Supports:
    - multi-message streams
    - partial buffering
    - replay-safe deterministic framing
    """

    HEADER_TERMINATOR = b"\r\n\r\n"

    def __init__(
        self,
        observer: ParserObserver | None = None,
    ) -> None:
        
        self.buffer: bytearray = bytearray()
        self.observer = observer

    def feed(self, data: bytes) -> Iterator[bytes]:
        """
        Accepts reconstructed TCP payload chunks
        and yields complete SIP header blocks.
        """

        self.buffer.extend(data)
        terminator_len = len(self.HEADER_TERMINATOR)

        while True:
            boundary = self.buffer.find(self.HEADER_TERMINATOR)

            if boundary == -1:
                return
            
            message_end = boundary + terminator_len

            message = bytes(self.buffer[:message_end])

            if self.observer:
                self.observer.on_message_reconstructed(
                    len(message),
                    datetime.utcnow(),
                )

            del self.buffer[:message_end]

            yield message


    def flush(self) -> bytes | None:
        """
        Returns remaining buffered data (if any).
        Used during session teardown.
        """

        if not self.buffer:
            return None
        
        remainder = bytes(self.buffer)
        self.buffer.clear()

        return remainder
