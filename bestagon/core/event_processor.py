import asyncio
import logging
from abc import abstractmethod, ABC
from asyncio import CancelledError
from typing import Union, Dict, Type, Callable

from bestagon.core.aggregate import DomainEvent
from bestagon.core.checkpoint_store import CheckpointStore, Checkpoint
from bestagon.core.event_store import EventStoreSubscription, EventStore
from bestagon.core.mapper import mapper
from bestagon.core.repository import EventSourcedRepository

logger = logging.getLogger(__name__)


class EventProcessor(ABC):
    def __init__(self, checkpoint_store: CheckpointStore):
        self._checkpoint_store = checkpoint_store
        self._subscription: Union[EventStoreSubscription, None] = None
        self._subscription_task: Union[asyncio.Task, None] = None

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def name(self) -> str:
        return self.get_name()

    @property
    def running(self) -> bool:
        if self.subscription is None:
            return True
        return self.subscription.running

    @property
    def subscription(self) -> Union[EventStoreSubscription, None]:
        return self._subscription

    @abstractmethod
    def get_event_routing(self) -> Dict[Type[DomainEvent], Callable]:
        """Reimplement to provide reaction to the specific domain event"""
        raise NotImplementedError

    def get_checkpoint_name(self) -> str:
        return f'{self.get_subscription_name()}_checkpoint'

    def get_subscription_name(self) -> str:
        return f'{self.get_name()}_subscription'

    async def process_event(self, event: DomainEvent) -> None:
        routing = self.get_event_routing()
        if not routing:
            logger.warning(f'Empty event routing for event_processor {self.name}')
        handler = routing.get(type(event))
        if handler is not None:
            await handler(event)

    async def _consume_subscription(self) -> None:
        while self.subscription.running:
            try:
                stream_event = await self.subscription.next_event()
                domain_event = mapper.to_domain_event(stream_event)
                checkpoint = Checkpoint(
                    name=self.get_checkpoint_name(),
                    value=stream_event.commit_position
                )

                # TODO - ACHTUNG - dangerous, what if only one operation will be completed??? Should be executed in a single transaction (how??? there are no Trasactions in event sourcing)
                # TODO - on exception system should be stoped
                await self.process_event(event=domain_event)
                await self.checkpoint_store.set_checkpoint(checkpoint)
            except StopAsyncIteration:
                logger.debug(f'Subscription {self.subscription.name} stopped.')
                break
            except Exception as e:
                logger.warning(f'ACHTUNG - event processor {self.name} critically failed:')
                logger.exception(e)
                await self.subscription.stop()
                raise e

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    async def stop(self) -> None:
        if self.subscription is None:
            return
        if self.subscription.running:
            await self.subscription.stop()
        if not self._subscription_task.done():
            self._subscription_task.cancel()

        try:
            await self._subscription_task
        except CancelledError:
            logger.debug(f'Task {self._subscription_task.get_name()} cancelled')

        self._subscription = None
        self._subscription_task = None

    async def subscribe_to(self, event_store: EventStore) -> None:
        if self.subscription is not None:
            logger.info(f'Event processor {self.name} already have a running subscription.')
            return

        event_types = list()
        for event_class in self.get_event_routing().keys():
            types = mapper.get_event_types(event_class)
            event_types.extend(types)

        if not event_types:
            logger.warning(f'Subscription cancelled. Event processor {self.name} have no declared events to listen to.')
            return

        subscription_name = self.get_subscription_name()
        checkpoint_name = self.get_checkpoint_name()

        checkpoint = await self.checkpoint_store.get_checkpoint(name=checkpoint_name)
        subscription = await event_store.create_subscription_to_events(
            subscription_name=subscription_name,
            events=event_types,
            start_position=checkpoint.value
        )

        self._subscription = subscription
        self._subscription_task = asyncio.create_task(self._consume_subscription(), name=f'{self.name}_subscription_task')
        logger.info(f'Event processor {self.name} subscribed to event store.')


class Application(EventProcessor):
    """
    The next step after you define your aggregates is to write an application.
    Aggregates serve as a central place for business logic, but they are nothing on their own. Aggregates should be able to communicate with other aggregates and be a part
    of use cases. This is where applcation comes in.

    Application is the place where you implement your use cases, starting from simple things like creation of aggregates and ending with complex cases which can involve
    interaction between multiple aggregates.
    """

    def __init__(self, repository: EventSourcedRepository, checkpoint_store: CheckpointStore):
        super().__init__(checkpoint_store=checkpoint_store)
        self._repository = repository

    @property
    def repository(self) -> EventSourcedRepository:
        return self._repository


class Projection(EventProcessor):
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    @abstractmethod
    async def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError

    async def initialize(self) -> None:
        pass
