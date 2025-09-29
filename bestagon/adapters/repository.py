from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, List

from bestagon.domain.domain_event import DomainEvent
from bestagon.adapters.event_store import EventStore, StoredEvent
from bestagon.exceptions import AggregateNotFoundError

if TYPE_CHECKING:
    from bestagon.domain.aggregate import Aggregate


@dataclass(frozen=True)
class StreamName:
    """Convention for stream name {SystemName}.{ApplicationName}.{AggregateType}-{AggregateId}"""
    system_name: str
    application_name: str
    aggregate_type: str
    aggregate_id: str

    def __eq__(self, other):
        if isinstance(other, StreamName):
            return self.to_string() == other.to_string()
        return NotImplemented

    def __post_init__(self):
        if not self.system_name:
            raise ValueError('System name cannot be empty.')
        if not self.application_name:
            raise ValueError('Application name cannot be empty.')
        if not self.aggregate_type:
            raise ValueError('Aggregate type cannot be empty.')
        if not self.aggregate_id:
            raise ValueError('Aggregate ID cannot be empty.')

    def __str__(self):
        return self.to_string()

    def to_string(self) -> str:
        return f'{self.system_name}.{self.application_name}.{self.aggregate_type}-{self.aggregate_id}'

    @classmethod
    def from_string(cls, s: str) -> 'StreamName':
        names, aggregate_id = s.split('-')
        system_name, application_name, aggregate_type = names.split('.')
        obj = cls(
            system_name=system_name,
            application_name=application_name,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id
        )
        return obj


class EventSourcedRepository(ABC):
    def __init__(self, event_store: EventStore, system_name: str, application_name: str):
        self._event_store = event_store
        self._system_name = system_name
        self._application_name = application_name

    @property
    def application_name(self) -> str:
        return self._application_name

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def system_name(self) -> str:
        return self._system_name

    @abstractmethod
    def from_stored_event(self, stored_event: StoredEvent) -> DomainEvent:
        raise NotImplementedError

    def get_by_id(self, aggregate_id: str) -> 'Aggregate':
        domain_events: List[DomainEvent] = list()
        stream_name = StreamName(
            system_name=self.system_name,
            application_name=self.application_name,
            aggregate_type=self.aggregate_class.get_type(),
            aggregate_id=aggregate_id
        )
        stored_events = self.event_store.get_stream(stream_name=stream_name.to_string())

        if not stored_events:
            raise AggregateNotFoundError(f'Aggregate {aggregate_id} not found.')

        for stored_event in stored_events:
            domain_event = self.from_stored_event(stored_event=stored_event)
            domain_events.append(domain_event)

        aggregate = self.reconstruct_aggregate(events=domain_events)
        return aggregate

    @abstractmethod
    def reconstruct_aggregate(self, events: List[DomainEvent]) -> 'Aggregate':
        raise NotImplementedError

    def save(self, aggregate: 'Aggregate') -> None:
        if not aggregate.pending_events:
            return

        stored_events = list()
        for domain_event in aggregate.pending_events:
            stored_event = self.to_stored_event(event=domain_event)
            stored_events.append(stored_event)

        stream_name = StreamName(
            system_name=self.system_name,
            application_name=self.application_name,
            aggregate_type=aggregate.get_type(),
            aggregate_id=aggregate.id
        )
        self.event_store.append_events(stream_name=stream_name.to_string(), events=stored_events)
        aggregate.clear_events()

    @abstractmethod
    def to_stored_event(self, event: DomainEvent) -> StoredEvent:
        raise NotImplementedError
