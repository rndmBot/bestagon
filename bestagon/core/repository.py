import datetime
import logging
from typing import List, Union
from uuid import uuid4

from bestagon.core.aggregate import Aggregate
from bestagon.core.event_store import EventStore
from bestagon.core.exceptions import BestagonError
from bestagon.core.message import DomainMessage, CommandMessage, DomainEventMetadata
from bestagon.core.registry import event_type_registry, aggregate_type_registry

logger = logging.getLogger(__name__)


class AggregateReconstructionError(BestagonError):
    pass


class AggregateNotFoundError(BestagonError):
    pass


class EventSourcedRepository:
    # TODO - docs
    # TODO - logging

    def __init__(self, event_store: EventStore):
        self._event_store = event_store

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    async def contains(self, aggregate_id: str) -> bool:
        return await self.event_store.stream_exists(stream_name=aggregate_id)

    async def get_by_id(self, aggregate_id: str) -> Aggregate:
        if not await self.event_store.stream_exists(aggregate_id):
            raise AggregateNotFoundError(
                f'Failed to retrieve aggregate: aggregate with ID {aggregate_id} not found.'
            )

        event_store_events = await self.event_store.get_stream(stream_name=aggregate_id)
        if not event_store_events:
            raise AggregateNotFoundError(
                f'Failed to retrieve aggregate: aggregate with ID {aggregate_id} not found.'
            )

        domain_messages: List[DomainMessage] = list()
        for event_store_event in event_store_events:
            domain_message = DomainMessage.from_event_store_event(event_store_event=event_store_event)
            domain_messages.append(domain_message)

        aggregate = self.reconstruct_aggregate(events=domain_messages)
        return aggregate

    @staticmethod
    def reconstruct_aggregate(events: List[DomainMessage]) -> Aggregate:
        first_message = events.pop(0)

        # TODO - do I really need this check?
        if not isinstance(first_message.event, Aggregate.Created):
            raise AggregateReconstructionError(
                f'Failed to reconstruct aggregate: '
                f'the first event must be an instance of <Aggregate.Created> class, got {type(first_message.event)}'
            )

        aggregate_class = aggregate_type_registry.get_aggregate_class(first_message.metadata.aggregate_type)
        aggregate = aggregate_class(
            event=first_message.event,
            aggregate_id=first_message.metadata.aggregate_id,
            aggregate_version=first_message.metadata.aggregate_version
        )
        for event in events:
            aggregate.mutate(event)
        return aggregate

    async def save(self, aggregate: Aggregate, trigger_message: Union[DomainMessage, CommandMessage]) -> None:
        logger.debug(f'Saving aggregate {aggregate.__class__.__qualname__} - {aggregate.aggregate_id}')
        events = aggregate.pending_events
        if not events:
            logger.debug(f'No new events to save for aggregate {aggregate.__class__.__qualname__} - {aggregate.aggregate_id}')
            return

        aggregate_version = aggregate.aggregate_version - len(events)
        messages = list()
        for event in events:
            if isinstance(trigger_message, DomainMessage):
                causation_id = trigger_message.metadata.event_id
            elif isinstance(trigger_message, CommandMessage):
                causation_id = trigger_message.metadata.command_id
            else:
                raise TypeError(f'Invalid trigger message type: {type(trigger_message)}')  # TODO - smells

            metadata = DomainEventMetadata(
                timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
                event_id=str(uuid4()),
                event_type=event_type_registry.get_event_types(type(event))[0],  # TODO - smells
                aggregate_id=aggregate.aggregate_id,
                aggregate_version=aggregate_version,
                aggregate_type=aggregate_type_registry.get_aggregate_types(type(aggregate))[0],  # TODO - smells

                correlation_id=trigger_message.metadata.correlation_id,
                causation_id=causation_id
            )
            aggregate_version += 1
            message = DomainMessage(
                event=event,
                metadata=metadata
            )
            messages.append(message)

        # Expected version
        original_aggregate_version = aggregate.aggregate_version - len(events)
        expected_version = None if original_aggregate_version == aggregate.INITIAL_VERSION else original_aggregate_version

        new_event_store_events = tuple(message.to_new_event_store_event() for message in messages)
        await self.event_store.append_events(
            stream_name=aggregate.aggregate_id,
            events=new_event_store_events,
            expected_version=expected_version
        )
        aggregate.clear_pending_events()
        logger.debug(f'Aggregate {aggregate.aggregate_id} successfully saved in repository.')
