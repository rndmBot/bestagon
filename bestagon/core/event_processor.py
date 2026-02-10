import asyncio
import logging
from abc import abstractmethod, ABC
from asyncio import CancelledError
from dataclasses import dataclass
from typing import Union

from bestagon.core.aggregate import DomainEvent
from bestagon.core.checkpoint_store import CheckpointStore, Checkpoint
from bestagon.core.event_store import EventStoreSubscription, EventStore
from bestagon.core.mapper import mapper
from bestagon.core.repository import EventSourcedRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Command:
    # TODO - commands sent through a command bus should allow the result to be returned
    pass


@dataclass(frozen=True)
class Query:
    pass


class EventProcessor(ABC):
    def __init__(self, checkpoint_store: CheckpointStore):
        self._checkpoint_store = checkpoint_store
        self._subscription: Union[EventStoreSubscription, None] = None
        self._task: Union[asyncio.Task, None] = None

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def name(self) -> str:
        return self.get_name()

    @property
    def subscription(self) -> Union[EventStoreSubscription, None]:
        return self._subscription

    @property
    def task(self) -> Union[asyncio.Task, None]:
        return self._task

    @abstractmethod
    async def process_event(self, event: DomainEvent) -> None:
        """Reimplement to provide reaction to the specific domain event"""
        raise NotImplementedError

    @staticmethod
    def _create_checkpoint_name(subscription_name: str) -> str:
        return f'{subscription_name}_checkpoint'

    async def _consume_subscription(self) -> None:
        while self.subscription.running:
            try:
                stream_event = await self.subscription.next_event()
                domain_event = mapper.to_domain_event(stream_event)
                checkpoint = Checkpoint(
                    name=self._create_checkpoint_name(self.subscription.name),
                    value=stream_event.commit_position
                )  # TODO - find da proppa wei to generate checkpoints

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
                raise e

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    async def stop(self) -> None:
        if self.subscription is None:
            return

        await self.subscription.stop()  # TODO - subscription should be deleted when stopped
        if not self._task.done():
            self._task.cancel()

        try:
            await self._task
        except CancelledError:
            logger.debug(f'Task {self._task.get_name()} cancelled')

        self._subscription = None
        self._task = None

    async def subscribe_to(self, event_store: EventStore) -> None:
        # TODO - add ability to subscribe only to events required by application
        if self.subscription is not None:
            logger.info(f'Application {self.name} already have a running subscription.')
            return

        # TODO - think again about subscription and checkpoint names
        subscription_name = f'{self.name}_subscription'
        checkpoint_name = self._create_checkpoint_name(subscription_name)

        checkpoint = await self.checkpoint_store.get_checkpoint(name=checkpoint_name)
        subscription = await event_store.create_subscription_to_all(
            subscription_name=checkpoint.name,
            start_position=checkpoint.value
        )

        self._subscription = subscription
        self._task = asyncio.create_task(self._consume_subscription(), name=f'{self.name}_subscription_task')
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
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError

    async def initialize(self) -> None:
        pass
