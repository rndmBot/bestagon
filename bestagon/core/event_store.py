from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import pairwise
from typing import List

from bestagon.exceptions import IntegrityError


@dataclass(frozen=True)
class NewStreamEvent:
    """New event to store in event store"""
    stream_position: int  # Position in aggreate sequence
    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class StreamEvent:
    """Event to be saved in and retreived from EventStore"""
    stream_name: str
    stream_position: int  # Position in aggreate sequence
    commit_position: int  # Position in application sequence
    event_type: str
    payload: bytes
    metadata: bytes

    def __eq__(self, other):
        if isinstance(other, StreamEvent):
            eq = all(
                [
                    self.stream_name == other.stream_name,
                    self.stream_position == other.stream_position
                ]
            )
            return eq
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, StreamEvent):
            return self.stream_position < other.stream_position
        return NotImplemented


class EventStore(ABC):
    # TODO - there are two ways to subscribe to events in event store - by event type and by stream name, event store should support both ways
    # TODO - IDEA - define EventReader and EventWriter interfaces, EventStore should inherit from both
    # TODO - Subscription by event type - EventSubscription
    # TODO - Subscription by stream name - StreamSubscription
    # TODO - subscription ideas https://eventuous.dev/docs/infra/esdb/

    def __contains__(self, item):
        return self.stream_exists(stream_name=item)

    @abstractmethod
    def append_events(self, stream_name: str, events: List[NewStreamEvent]) -> None:
        """
        Adds new events to specified event stream.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_events(self, regex_list: List[str], start_position: int, limit: int) -> List[StreamEvent]:
        """Allows to retreive events from specific streams specified by regex, starting from specific position."""
        raise NotImplementedError

    @abstractmethod
    def get_stream(self, stream_name: str) -> List[StreamEvent]:
        """Returns all events from a single stream."""
        raise NotImplementedError

    @abstractmethod
    def stream_exists(self, stream_name: str) -> bool:
        """Returns True if specified stream exists in event store."""
        raise NotImplementedError
