from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List

from bestagon.domain.domain_event import DomainEvent
from bestagon.adapters.event_store import EventStore
from bestagon.exceptions import AggregateNotFoundError
from bestagon.adapters.stored_event import StoredEvent

if TYPE_CHECKING:
    from bestagon.domain.aggregate import Aggregate


class EventSourcedRepository(ABC):
    # TODO - IDEA - stream name {Context}.{Application}.{Aggregate}-{ID}

    def __init__(self, event_store: EventStore, stream_name_prefix: str):
        self._event_store = event_store
        self._stream_name_prefix = stream_name_prefix

    def __contains__(self, item):
        return self.contains(aggregate_id=item)

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def stream_name_prefix(self) -> str:
        return self._stream_name_prefix

    def contains(self, aggregate_id: str) -> bool:
        stream_name = self.create_stream_name(aggregate_id=aggregate_id)
        return self.event_store.stream_exists(stream_name=stream_name)

    def create_stream_name(self, aggregate_id: str) -> str:
        # {Schema}.{Category}-{Id:n}
        if self.stream_name_prefix:
            stream_name = f'{self.stream_name_prefix}-{aggregate_id}'
            return stream_name
        return aggregate_id

    @abstractmethod
    def from_stored_event(self, stored_event: StoredEvent) -> DomainEvent:
        raise NotImplementedError

    def get_by_id(self, aggregate_id: str) -> 'Aggregate':
        domain_events: List[DomainEvent] = list()
        stream_name = self.create_stream_name(aggregate_id=aggregate_id)
        stored_events = self.event_store.get_events(stream_name=stream_name)

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

        stream_name = self.create_stream_name(aggregate_id=aggregate.id)
        self.event_store.append_events(stream_name=stream_name, events=stored_events)
        aggregate.clear_events()

    @abstractmethod
    def to_stored_event(self, event: DomainEvent) -> StoredEvent:
        raise NotImplementedError
