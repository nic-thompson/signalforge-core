from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, Tuple, Optional
import uuid

SessionKey = Tuple[str, int, str, int]

@dataclass
class TCPSession:
    session_id: str
    source_ip: str
    destination_ip: str
    source_port: int
    destination_port: int
    start_timestamp: datetime
    last_activity_timestamp: datetime
    end_timestamp: datetime | None = None

    expected_sequence: int = 0
    buffered_segments: Dict[int, bytes] = field(default_factory=dict)

    def update_activity(self, timestamp: datetime) -> None:
        self.last_activity_timestamp = timestamp

    def close(self, timestamp: datetime) -> None:
        self.end_timestamp = timestamp
    


class SessionTracker:
    """
    Maintains canonical TCP session state across bidirectional packet flows,
    supporting deterministic identification, incremental updates, and replay-safe ordering.
    
    Supports:
    - deterministic session identification
    - incremental updates
    - replay-safe ordering
    """

    def __init__(self) -> None:
        self.sessions: Dict[SessionKey, TCPSession] = {}

    @staticmethod
    def _normalise_session_key(
        src_ip: str,
        src_port: int,
        dst_ip: str,
        dst_port: int,
    ) -> SessionKey:
        """
        Ensures bidirectional traffic maps to same session.
        """

        if (src_ip, src_port) <= (dst_ip, dst_port):
            return src_ip, src_port, dst_ip, dst_port
        
        return dst_ip, dst_port, src_ip, src_port
    
    def get_or_create_session(
            self,
            src_ip: str,
            src_port: int,
            dst_ip: str,
            dst_port: int,
            timestamp: datetime,
    ) -> TCPSession:
        
        key = self._normalise_session_key(
            src_ip,
            src_port,
            dst_ip,
            dst_port,
        )

        if key not in self.sessions:
            session = TCPSession(
                session_id = f"{key}-{timestamp.timestamp()}",
                source_ip=key[0],
                source_port=key[1],
                destination_ip=key[2],
                destination_port=key[3],
                start_timestamp=timestamp,
                last_activity_timestamp=timestamp,
            )

            self.sessions[key] = session

        else:
            session = self.sessions[key]
            session.update_activity(timestamp)


        return session
    
    def close_session(
            self,
            src_ip: str,
            src_port: int,
            dst_ip: str,
            dst_port: int,
            timestamp: datetime,
    ) -> None:
        
        key = self._normalise_session_key(
            src_ip,
            src_port,
            dst_ip,
            dst_port,
        )

        session = self.sessions.get(key)

        if session:
            session.close(timestamp)

    def active_sessions(self) -> Iterable[TCPSession]:

        return (
            session
            for session in self.sessions.values()
            if session.end_timestamp is None
        )
        


