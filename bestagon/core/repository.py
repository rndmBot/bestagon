import logging
from itertools import pairwise
from typing import List, Tuple

from bestagon.core.aggregate import Aggregate, DomainEvent
from bestagon.core.mapper import mapper
from bestagon.core.event_store import EventStore
from bestagon.core.exceptions import AggregateNotFoundError, AggregateVersionError

logger = logging.getLogger(__name__)


class EventSourcedRepository:
    # TODO - logging
    def __init__(self, event_store: EventStore):
        self._event_store = event_store

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @staticmethod
    def _create_stream_name(aggregate_type: str, aggregate_id: str) -> str:
        return f'{aggregate_type}-{aggregate_id}'

    @staticmethod
    def _validate_events(events: Tuple[DomainEvent, ...]) -> None:
        # Events cannot be empty list
        if not events:
            raise ValueError(
                f'Events validation failed - events should be a non empty list of {DomainEvent.__class__.__qualname__} instances.'
            )

        # Events should be a list of DomainEvent class
        if not all([isinstance(event, DomainEvent) for event in events]):
            raise TypeError(
                f'Events validation failed - all appended events should be instances of {DomainEvent.__class__.__qualname__} class.'
            )

        # Event versions should be monotonically increasing
        stream_positions = [event.metadata.aggregate_version for event in events]
        diffs = [y - x for x, y in pairwise(stream_positions)]
        gapless = all([True if d == 1 else False for d in diffs])
        if not gapless:
            raise AggregateVersionError(
                'Events validation failed - aggregate verions of events should be a monotonically increasing sequence of integers, '
                f'provided events contain events out of order or/and events with gaps in aggregate version: {stream_positions}'
            )

    async def contains(self, aggregate_type: str, aggregate_id: str) -> bool:
        stream_id = self._create_stream_name(aggregate_type=aggregate_type, aggregate_id=aggregate_id)
        return await self.event_store.stream_exists(stream_name=stream_id)

    async def get_by_id(self, aggregate_type: str, aggregate_id: str) -> Aggregate:
        domain_events: List[DomainEvent] = list()
        stream_name = self._create_stream_name(aggregate_type=aggregate_type, aggregate_id=aggregate_id)
        if not await self.event_store.stream_exists(stream_name):
            raise AggregateNotFoundError(f'Aggregate {aggregate_id} not found.')

        stored_events = await self.event_store.get_stream(stream_name=stream_name)
        if not stored_events:
            raise AggregateNotFoundError(f'Aggregate {aggregate_id} not found.')

        for stored_event in stored_events:
            domain_event = mapper.to_domain_event(stream_event=stored_event)
            domain_events.append(domain_event)

        aggregate = self.reconstruct_aggregate(events=domain_events)
        return aggregate

    @staticmethod
    def reconstruct_aggregate(events: List[DomainEvent]) -> Aggregate:
        created = events.pop(0)
        if not isinstance(created, Aggregate.Created):
            raise TypeError(f'Invalid event type, expected instance of class <Aggregate.Created>, got {type(created)}')

        aggregate_class = mapper.get_aggregate_class(aggregate_type=created.metadata.aggregate_type)
        aggregate = aggregate_class(created)
        for event in events:
            aggregate.mutate(event)
        return aggregate

    async def save(self, aggregate: Aggregate) -> None:
        events = aggregate.pending_events
        if not events:
            return
        self._validate_events(events=events)

        last_event_version = events[0].metadata.aggregate_version
        expected_version = None if last_event_version == 0 else last_event_version - 1
        stream_name = self._create_stream_name(aggregate_type=aggregate.get_aggregate_type(), aggregate_id=aggregate.aggregate_id)

        new_stored_events = tuple(
            mapper.to_new_event_store_event(domain_event) for domain_event in aggregate.pending_events)
        await self.event_store.append_events(
            stream_name=stream_name,
            events=new_stored_events,
            expected_version=expected_version
        )
        aggregate.clear_events()
