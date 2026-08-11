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

    def _validate_events(self, stream_version: int, events: Tuple[DomainEvent, ...]) -> None:
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

        first_event = events[0]

        # If stream exists then the first event's aggregate_version should be exactly one more than the current stream version
        if stream_version >= 0:
            if first_event.metadata.aggregate_version <= stream_version:
                raise AggregateVersionError(
                    f'Events validation failed - the first event\'s aggregate version is less or equal to the current stream version. '
                    f'This means that there is already event recorded in the stream in that position, and new events possibly contain duplicates.'
                )

            if first_event.metadata.aggregate_version - stream_version != 1:
                raise AggregateVersionError(
                    f'Events validation failed - the first event\'s aggregate version should be exactly one more than the current stream version: '
                    f'current stream version - {stream_version}, first event aggregate version - {first_event.metadata.aggregate_version}'
                )
        # If stream version is negative, then the stream does not exists, in such case the first event should have aggregate version 0
        else:
            if first_event.metadata.aggregate_version != 0:
                raise AggregateVersionError(
                    f'Events validation failed - aggregate version of the first event for non existing stream should be 0, '
                    f'got - {first_event.metadata.aggregate_version}'
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

        stream_name = self._create_stream_name(
            aggregate_type=aggregate.get_aggregate_type(),
            aggregate_id=aggregate.aggregate_id
        )
        stream_version = await self.event_store.get_stream_version(stream_name=stream_name)
        self._validate_events(stream_version=stream_version, events=events)
        new_stored_events = tuple(mapper.to_new_stream_event(domain_event) for domain_event in aggregate.pending_events)
        await self.event_store.append_events(stream_name=stream_name, events=new_stored_events)
        aggregate.clear_events()
