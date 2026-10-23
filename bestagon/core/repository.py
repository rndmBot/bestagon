import datetime
import logging
from typing import List, Union, Type
from uuid import uuid4

from bestagon.core.aggregate import Aggregate
from bestagon.core.event_store import EventStore
from bestagon.core.exceptions import BestagonError
from bestagon.core.mapper import to_domain_message, to_new_event_store_event
from bestagon.core.message import DomainMessage, CommandMessage, DomainEventMetadata
from bestagon.core.registry import aggregate_type_registry

logger = logging.getLogger(__name__)


class AggregateReconstructionError(BestagonError):
    pass


class AggregateNotFoundError(BestagonError):
    pass


class EventSourcedRepository:
    # TODO - the repository should provide a way to define how exactly stream name will be constructed

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
            domain_message = to_domain_message(event_store_event=event_store_event)
            domain_messages.append(domain_message)

        aggregate = self.reconstruct_aggregate(messages=domain_messages)
        return aggregate

    @staticmethod
    def reconstruct_aggregate(messages: List[DomainMessage]) -> Aggregate:
        first_message = messages.pop(0)

        # TODO - is it really necessary to check the first event?
        if not isinstance(first_message.event, Aggregate.AggregateCreated):
            raise AggregateReconstructionError(
                f'Failed to reconstruct aggregate: '
                f'the first event must be an instance of <Aggregate.AggregateCreated> class, got {type(first_message.event)}'
            )

        aggregate_class: Type[Aggregate] = aggregate_type_registry.get_aggregate_class(first_message.metadata.aggregate_type)
        aggregate: Aggregate = aggregate_class(
            event=first_message.event,
            aggregate_id=first_message.metadata.aggregate_id,
            aggregate_version=first_message.metadata.aggregate_version
        )
        for message in messages:
            aggregate.mutate(message)
        return aggregate

    async def save(self, aggregate: Aggregate, trigger_message: Union[DomainMessage, CommandMessage]) -> None:
        logger.debug(f'Saving aggregate {aggregate.__class__.__qualname__} - {aggregate.aggregate_id}')
        pending_events = aggregate.pending_events
        if not pending_events:
            logger.debug(f'No new events to save for aggregate {aggregate.__class__.__qualname__} - {aggregate.aggregate_id}')
            return

        messages: List[DomainMessage] = list()
        for pending_event in pending_events:

            # ----------------------------------------- #
            # TODO - metadata creation process should be improved
            #   as an idea it should be extracted to a separate component, for example MetadataFactory,
            #   that will be responsible for creation, serialization and deserialization of metadata
            if isinstance(trigger_message, DomainMessage):
                causation_id = trigger_message.metadata.event_id
            elif isinstance(trigger_message, CommandMessage):
                causation_id = trigger_message.metadata.command_id
            else:
                raise TypeError(f'Invalid trigger message type: {type(trigger_message)}')

            metadata = DomainEventMetadata(
                timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
                event_id=str(uuid4()),
                event_type=pending_event.domain_event.event_type,
                aggregate_id=aggregate.aggregate_id,
                aggregate_version=pending_event.aggregate_version,
                aggregate_type=aggregate.aggregate_type,

                correlation_id=trigger_message.metadata.correlation_id,
                causation_id=causation_id
            )
            # ----------------------------------------- #

            message = DomainMessage(
                event=pending_event.domain_event,
                metadata=metadata
            )
            messages.append(message)

        new_event_store_events = tuple(to_new_event_store_event(message) for message in messages)
        await self.event_store.append_events(
            stream_name=aggregate.aggregate_id,
            events=new_event_store_events,
            expected_version=aggregate.original_version
        )
        aggregate.clear_pending_events()
        logger.debug(f'Aggregate {aggregate.aggregate_id} successfully saved in repository.')
