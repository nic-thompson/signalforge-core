from dataclasses import dataclass
from typing import Dict

from telemetry_parser.observability.parser_observer import ParserObserver

class MalformedSIPMessageError(Exception):
    pass

@dataclass
class SIPMessage:
    method: str
    headers: Dict[str, str]
    device_id: str | None
    call_id: str | None
    transport: str | None
    source_ip: str | None


class SIPParser:
    """
    Parses SIP messages into structured protocol objects.

    Supports:
    - REGISTER parsing
    - header extraction
    - malformed tolerance
    """

    def __init__(
        self,
        observer: ParserObserver | None = None
    ) -> None:
        self.observer = observer


    def parse(self, raw_message: bytes) -> SIPMessage | None:

        try:
            text = raw_message.decode(errors="ignore")
        except Exception:

            if self.observer:
                self.observer.on_parse_error(
                    "decode_failure",
                    len(raw_message),
                )
                
            return None
        
        lines = text.split("\r\n")

        if not lines:
            return None
        
        request_line = lines[0]

        parts = request_line.split()

        if len(parts) < 3:
            return None
        
        method = parts[0].upper()

        headers = self._parse_headers(lines[1:])

        device_id = self._extract_device_id(headers)

        call_id = headers.get("call-id")

        transport, source_ip = self._extract_transport(headers)

        return SIPMessage(
            method=method,
            headers=headers,
            device_id=device_id,
            call_id=call_id,
            transport=transport,
            source_ip=source_ip,
        )


    def _parse_headers(self, lines: list[str]) -> Dict[str, str]:

        headers: Dict[str, str] = {}

        for line in lines:

            if not line.strip():
                continue

            if ":" not in line:
                continue
            
            key, value = line.split(":", 1)

            headers[key.strip().lower()] = value.strip()

        return headers


    def _extract_device_id(
        self,
        headers: Dict[str, str],
    ) -> str | None:
        
        from_header = headers.get("from")

        if not from_header:
            return None
        
        if "sip:" not in from_header:
            return None
        
        try:
            identifier = from_header.split("sip:")[1]
            identifier = identifier.split("@")[0]
            identifier = identifier.replace("<", "").replace(">", "")
            return identifier
        except Exception:
            return None
    

    def _extract_transport(
            self,
            headers: Dict[str, str],
    ) -> tuple[str | None, str | None]:
        
        via = headers.get("via")

        if not via:
            return None, None
        
        try:
            
            parts = via.split()

            if len(parts) < 2:
                return None, None
            
            protocol_part = parts[0]

            transport = protocol_part.split("/")[-1]

            address_part = parts[1]

            source_ip = address_part.split(":")[0]

            return transport, source_ip

        except Exception:

            return None, None 

