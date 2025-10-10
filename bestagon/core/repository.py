import logging
from itertools import pairwise
from typing import TYPE_CHECKING, List

from bestagon.core.mapper import Mapper
from bestagon.core.policy import StreamNamePolicy
from bestagon.domain.domain_event import DomainEvent
from bestagon.core.event_store import EventStore
from bestagon.exceptions import AggregateNotFoundError, IntegrityError

if TYPE_CHECKING:
    from bestagon.domain.aggregate import Aggregate


logger = logging.getLogger(__name__)


class EventSourcedRepository:
    def __init__(self, event_store: EventStore, stream_name_policy: StreamNamePolicy, mapper: Mapper):
        self._event_store = event_store
        self._stream_name_policy = stream_name_policy
        self._mapper = mapper

    def __contains__(self, item):
        return self.contains(item)

    @property
    def mapper(self) -> Mapper:
        return self._mapper

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def stream_name_policy(self) -> StreamNamePolicy:
        return self._stream_name_policy

    def contains(self, aggregate_id) -> bool:
        stream_id = self.stream_name_policy.create_stream_name(aggregate_id=aggregate_id)
        return self.event_store.stream_exists(stream_name=stream_id)

    def get_by_id(self, aggregate_id: str) -> 'Aggregate':
        domain_events: List[DomainEvent] = list()
        stream_name = self.stream_name_policy.create_stream_name(aggregate_id=aggregate_id)
        stored_events = self.event_store.get_stream(stream_name=stream_name)

        if not stored_events:
            raise AggregateNotFoundError(f'Aggregate {aggregate_id} not found.')

        for stored_event in stored_events:
            domain_event = self.mapper.to_domain_event(stream_event=stored_event)
            domain_events.append(domain_event)

        aggregate = self.reconstruct_aggregate(events=domain_events)
        return aggregate

    def reconstruct_aggregate(self, events: List[DomainEvent]) -> 'Aggregate':
        created = events.pop(0)
        if not isinstance(created, Aggregate.Created):
            raise TypeError(f'Invalid event type, expected instance of class <Aggregate.Created>, got {type(created)}')

        aggregate_class = self.mapper.get_aggregate_class(aggregate_type=created.metadata.aggregate_type)
        aggregate = aggregate_class(created)
        for event in events:
            aggregate.mutate(event)
        return aggregate

    def save(self, aggregate: 'Aggregate') -> None:
        if not aggregate.pending_events:
            return

        # Events validation - aggregate version must be monotonically increasing
        versions = [event.metadata.aggregate_version for event in aggregate.pending_events]
        diffs = [y - x for x, y in pairwise(versions)]
        gapless = all([True if d == 1 else False for d in diffs])
        if not gapless:
            logger.debug(f'{aggregate.id}: {versions}')
            raise IntegrityError('Invalid aggregate version: each aggregate event should have version exactly one more than previous.')

        new_stored_events = list()
        for domain_event in aggregate.pending_events:
            new_stored_event = self.mapper.to_new_stream_event(domain_event=domain_event)
            new_stored_events.append(new_stored_event)

        stream_name = self.stream_name_policy.create_stream_name(aggregate_id=aggregate.id)
        self.event_store.append_events(stream_name=stream_name, events=new_stored_events)
        aggregate.clear_events()
