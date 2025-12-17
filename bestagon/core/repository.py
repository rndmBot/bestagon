import logging
from itertools import pairwise
from typing import List

from bestagon.core.aggregate import Aggregate
from bestagon.core.mapper import mapper
from bestagon.core.message import DomainEvent
from bestagon.core.event_store import AsyncEventStore
from bestagon.exceptions import AggregateNotFoundError, IntegrityError

logger = logging.getLogger(__name__)


class AsyncRepository:
    def __init__(self, event_store: AsyncEventStore):
        self._event_store = event_store

    @property
    def event_store(self) -> AsyncEventStore:
        return self._event_store

    @staticmethod
    def _create_stream_name(aggregate_type: str, aggregate_id: str) -> str:
        return f'{aggregate_type}-{aggregate_id}'

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
        if not aggregate.pending_events:
            return

        # Events validation - aggregate version must be monotonically increasing
        versions = [event.metadata.aggregate_version for event in aggregate.pending_events]
        diffs = [y - x for x, y in pairwise(versions)]
        gapless = all([True if d == 1 else False for d in diffs])
        if not gapless:
            logger.debug(f'{aggregate.aggregate_id}: {versions}')
            raise IntegrityError('Invalid aggregate version: each aggregate event should have version exactly one more than previous.')

        new_stored_events = list()
        for domain_event in aggregate.pending_events:
            new_stored_event = mapper.to_new_stream_event(domain_event=domain_event)
            new_stored_events.append(new_stored_event)

        stream_name = self._create_stream_name(aggregate_type=aggregate.get_aggregate_type(), aggregate_id=aggregate.aggregate_id)
        await self.event_store.append_events(stream_name=stream_name, events=new_stored_events)
        aggregate.clear_events()
