from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class StoredEvent:
    """Event to be saved in and retreived from EventStore"""
    stream_name: str
    stream_position: int  # Position in aggreate sequence
    commit_position: Optional[int]  # Position in application sequence
    event_type: str
    payload: bytes
    metadata: bytes

    def __eq__(self, other):
        if isinstance(other, StoredEvent):
            eq = all(
                [
                    self.stream_name == other.stream_name,
                    self.stream_position == other.stream_position
                ]
            )
            return eq
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, StoredEvent):
            return self.stream_position < other.stream_position
        return NotImplemented
