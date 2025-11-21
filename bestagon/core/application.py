import asyncio
import logging
from abc import abstractmethod, ABC


from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_store import AsyncEventStore
from bestagon.core.message import DomainEvent
from bestagon.core.repository import AsyncRepository
from bestagon.core.subscription import AsyncDomainEventsSubscription

logger = logging.getLogger(__name__)


class EventProcessor(ABC):
    def __init__(self, name: str, checkpoint_store: CheckpointStore):
        self._name = name
        self._checkpoint_store = checkpoint_store
        self._subscription = None

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def name(self) -> str:
        return self._name

    @property
    def subscription(self) -> AsyncDomainEventsSubscription:
        return self._subscription

    @abstractmethod
    async def process_event(self, event: DomainEvent) -> None:
        raise NotImplementedError

    async def _consume_subscription(self) -> None:
        # TODO - rethink, maybe there is a better way to consume events???
        # TODO - intercept asyncio.CancelledError to shutdown gracefully

        while self.subscription.running:
            application_event = await self.subscription.next_event()

            # TODO - ACHTUNG - dangerous, what if only one operation will be completed???
            await self.process_event(event=application_event.domain_event)
            await self.checkpoint_store.set_checkpoint(name=self.subscription.subscription_id, value=application_event.commit_position)

    def set_subscription(self, subscription: AsyncDomainEventsSubscription) -> asyncio.Task:
        if self.subscription is not None:
            raise ValueError(f'Failed to set subscription for {self.name} - only one subspcription can be set for event processor.')
        self._subscription = subscription
        task = asyncio.create_task(self._consume_subscription(), name=f'{self.name}_subscription_task')
        return task

    async def stop(self) -> None:
        await self.subscription.stop()


class Application(EventProcessor):
    def __init__(self, name: str, event_store: AsyncEventStore, checkpoint_store: CheckpointStore):
        super().__init__(name=name, checkpoint_store=checkpoint_store)
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
