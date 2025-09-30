from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, List

from bestagon.core.event_converter import EventConverter
from bestagon.domain.domain_event import DomainEvent
from bestagon.core.event_store import EventStore
from bestagon.exceptions import AggregateNotFoundError

if TYPE_CHECKING:
    from bestagon.domain.aggregate import Aggregate


@dataclass(frozen=True)
class ConventionStreamName:
    """Convention for stream name {SystemName}.{ApplicationName}.{AggregateType}-{AggregateId}"""
    system_name: str
    application_name: str
    aggregate_type: str
    aggregate_id: str

    def __eq__(self, other):
        if isinstance(other, ConventionStreamName):
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
    def from_string(cls, s: str) -> 'ConventionStreamName':
        names, aggregate_id = s.split('-')
        system_name, application_name, aggregate_type = names.split('.')
        obj = cls(
            system_name=system_name,
            application_name=application_name,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id
        )
        return obj


class StreamNamePolicy(ABC):
    @abstractmethod
    def create_stream_name(self, aggregate_id: str) -> str:
        raise NotImplementedError


class ConventionStreamNamePolicy(StreamNamePolicy):
    def __init__(self, system_name: str, application_name: str, aggregate_type: str):
        self._system_name = system_name
        self._application_name = application_name
        self._aggregate_type = aggregate_type

    @property
    def aggregate_type(self) -> str:
        return self._aggregate_type

    @property
    def application_name(self) -> str:
        return self._application_name

    @property
    def system_name(self) -> str:
        return self._system_name

    def create_stream_name(self, aggregate_id: str) -> str:
        stream_name = ConventionStreamName(
            system_name=self.system_name,
            application_name=self.application_name,
            aggregate_type=self.aggregate_type,
            aggregate_id=aggregate_id
        )
        return stream_name.to_string()


class EventSourcedRepository(ABC):
    def __init__(self, event_store: EventStore, stream_name_policy: StreamNamePolicy, event_converter: EventConverter):
        self._event_store = event_store
        self._stream_name_policy = stream_name_policy
        self._event_converter = event_converter

    @property
    def event_converter(self) -> EventConverter:
        return self._event_converter

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def stream_name_policy(self) -> StreamNamePolicy:
        return self._stream_name_policy

    def get_by_id(self, aggregate_id: str) -> 'Aggregate':
        domain_events: List[DomainEvent] = list()
        stream_name = self.stream_name_policy.create_stream_name(aggregate_id=aggregate_id)
        stored_events = self.event_store.get_stream(stream_name=stream_name)

        if not stored_events:
            raise AggregateNotFoundError(f'Aggregate {aggregate_id} not found.')

        for stored_event in stored_events:
            domain_event = self.event_converter.from_stored_event(stored_event=stored_event)
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
            stored_event = self.event_converter.to_stored_event(domain_event=domain_event)
            stored_events.append(stored_event)

        stream_name = self.stream_name_policy.create_stream_name(aggregate_id=aggregate.id)
        self.event_store.append_events(stream_name=stream_name, events=stored_events)
        aggregate.clear_events()
