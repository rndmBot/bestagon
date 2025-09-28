from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

from bestagon.exceptions import AggregateIDMismatch, AggregateVersionError
from bestagon.domain.domain_event import DomainEvent, Created


class Aggregate(ABC):
    INITIAL_VERSION = 0

    @dataclass(frozen=True)
    class Event(DomainEvent):
        """Convenience class"""

        pass

    @dataclass(frozen=True)
    class Created(Created):
        """Convenience class"""

        pass

    def __init__(self, event: Created):
        self._id = event.aggregate_id
        self._version = event.aggregate_version
        self._created_on = datetime.fromisoformat(event.timestamp)
        self._modified_on = datetime.fromisoformat(event.timestamp)

        self._pending_events: List[DomainEvent] = list()

    @property
    def created_on(self) -> datetime:
        """The date and time when the aggregate was created."""
        return self._created_on

    @property
    def id(self) -> str:
        """The ID of the aggregate."""
        return self._id

    @property
    def modified_on(self) -> datetime:
        """The date and time when the aggregate was last modified."""
        return self._modified_on

    @property
    def next_version(self) -> int:
        """Convenience property to get the next version number of aggregate"""
        return self.version + 1

    @property
    def pending_events(self) -> Tuple[DomainEvent, ...]:
        return tuple(self._pending_events)

    @property
    def version(self) -> int:
        return self._version

    @classmethod
    def _create(cls, event: 'Created') -> 'Aggregate':
        obj = cls(event=event)
        obj._pending_events.append(event)
        return obj

    def apply_event(self, event: DomainEvent) -> None:
        raise TypeError(f'Unknown Event {type(event)}')

    def clear_events(self) -> None:
        self._pending_events = list()

    @staticmethod
    @abstractmethod
    def create_id(*args, **kwargs) -> str:
        raise NotImplementedError

    def mutate(self, event: DomainEvent) -> None:
        """
        Validation:
            - Event.aggregate_id should be equal to Aggregate.id. Raises AggregateIdError if not.
            - Event.aggregate_version should be EXACTLY ONE more than Aggregate.version. Raises AggregateVersionError if not.
        """
        # Event validation
        if self.id != event.aggregate_id:
            raise AggregateIDMismatch(
                f'ERROR - {event.__class__.__qualname__} aggregate_id does not match Aggregate ID attribute.')
        elif (event.aggregate_version - self.version) != 1:
            raise AggregateVersionError(
                f'ERROR - event aggregate_version should be exactly one more than aggregate version.')

        # Change the state of Aggregate
        self.apply_event(event)

        # Event post processing
        self._version = event.aggregate_version
        self._modified_on = event.timestamp

    def trigger_event(self, event: Event) -> None:
        """Mutates aggregate state and adds event to the list of pending events"""
        self.mutate(event)
        self._pending_events.append(event)
