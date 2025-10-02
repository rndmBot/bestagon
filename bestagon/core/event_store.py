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

    def __eq__(self, other):
        if isinstance(other, NewStreamEvent):
            return self.stream_position == other.stream_position
        return NotImplemented


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

    @staticmethod
    def _check_events_gapless(events: List[NewStreamEvent]) -> None:
        """
        Events should have monotonicaly increasing stream position, ex: 0, 1, 2, 3
        Any gaps or out of order events are not allowed.
        """
        versions = [event.stream_position for event in events]
        diffs = [y - x for x, y in pairwise(versions)]
        gapless = all([True if d == 1 else False for d in diffs])
        if not gapless:
            raise IntegrityError('Events must be gapless to record in event store.')

    @staticmethod
    def _check_events_unique(events: List[NewStreamEvent]) -> None:
        """Events must be unique to be recorded in event store."""
        if len(set(events)) < len(events):
            raise IntegrityError('Events must be unique to record to event store.')

    @abstractmethod
    def append_events(self, stream_name: str, events: List[NewStreamEvent]) -> None:
        """
        Adds new events to specified event stream.
        ACHTUNG - validate events before recording them into the event store.
        """
        raise NotImplementedError

    def validate_new_events(self, events: List[NewStreamEvent]) -> None:
        """Convenience class."""
        self._check_events_unique(events)
        self._check_events_gapless(events)

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_events(self, regex_list: List[str], start_position: int, limit: int) -> List[StreamEvent]:
        """Allows to retreive events from specific streams specified by regex, strating from specific position."""
        raise NotImplementedError

    @abstractmethod
    def get_stream(self, stream_name: str) -> List[StreamEvent]:
        """Returns all events from a single stream."""
        raise NotImplementedError

    @abstractmethod
    def stream_exists(self, stream_name: str) -> bool:
        """Returns True if specified stream exists in event store."""
        raise NotImplementedError
