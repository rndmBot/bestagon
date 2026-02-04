import asyncio
import logging
from abc import abstractmethod, ABC
from asyncio import CancelledError
from typing import Union

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_store import EventStore, ApplicationSubscription
from bestagon.core.message import DomainEvent
from bestagon.core.repository import AsyncRepository

logger = logging.getLogger(__name__)


class EventProcessor(ABC):
    def __init__(self, checkpoint_store: CheckpointStore):
        self._checkpoint_store = checkpoint_store
        self._subscription: Union[ApplicationSubscription, None] = None
        self._task: Union[asyncio.Task, None] = None

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def name(self) -> str:
        return self.get_name()

    @property
    def subscription(self) -> Union[ApplicationSubscription, None]:
        return self._subscription

    @property
    def task(self) -> Union[asyncio.Task, None]:
        return self._task

    @abstractmethod
    async def process_event(self, event: DomainEvent) -> None:
        """Reimplement to provide reaction to the specific domain event"""
        raise NotImplementedError

    async def _consume_subscription(self) -> None:
        while self.subscription.running:
            try:
                application_event = await self.subscription.next_event()
            except StopAsyncIteration:
                logger.debug(f'Subscription {self.subscription.subscription_id} stopped.')
                break

            # TODO - ACHTUNG - dangerous, what if only one operation will be completed??? Should be executed in a single transaction (how??? there are no Trasactions in event sourcing)
            # TODO - ORLY - add unit of work to coordinate event processing or make em synchronous???
            # TODO - on exception system should be stoped
            try:
                await self.process_event(event=application_event.domain_event)
                await self.checkpoint_store.set_checkpoint(name=self.subscription.subscription_id, value=application_event.commit_position)
            except Exception as e:
                logger.warning(f'ACHTUNG - event processor {self.name} critically failed:')
                logger.exception(e)
                raise e

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    def set_subscription(self, subscription: ApplicationSubscription) -> None:
        """Only one subscription currently allowed"""
        if self.subscription is not None:
            raise ValueError(f'Failed to set subscription for {self.name} - only one subspcription can be set for event processor.')
        self._subscription = subscription
        self._task = asyncio.create_task(self._consume_subscription(), name=f'{self.name}_subscription_task')

    async def stop(self) -> None:
        # Do nothing if there is no subscription
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


class Application(EventProcessor):
    """
    The next step after you define your aggregates is to write an application.
    Aggregates serve as a central place for business logic, but they are nothing on their own. Aggregates should be able to communicate with other aggregates and be a part
    of use cases. This is where applcation comes in.

    Application is the place where you implement your use cases, starting from simple things like creation of aggregates and ending with complex cases which can involve
    interaction between multiple aggregates.
    """

    def __init__(self, event_store: EventStore, checkpoint_store: CheckpointStore):
        super().__init__(checkpoint_store=checkpoint_store)
        self._repository = AsyncRepository(event_store=event_store)

    @property
    def repository(self) -> AsyncRepository:
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
