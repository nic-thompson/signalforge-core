"""
Carriers that keep packet observation time attached to stream data.

The reassembler and decoder both buffer bytes, and both used to hand on
bare ``bytes`` — which discarded the only trustworthy record of when a
message was seen on the wire. Event time then fell back to ingestion
wall-clock in the normaliser, which is not reproducible on replay.

These types keep the observation timestamp travelling with the bytes it
belongs to. See docs/ADR-001-edge-producer-contract.md.

Both carry the timestamp of the packet that supplied the *first* byte,
per ADR-001: a message is observed when its first byte arrives, and that
instant does not move if a later segment is retransmitted.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TimestampedChunk:
    """Reassembled stream bytes with the observation time of their first byte."""

    timestamp: datetime
    data: bytes


@dataclass(frozen=True)
class TimestampedMessage:
    """A framed SIP message with the observation time of its first byte."""

    timestamp: datetime
    data: bytes
