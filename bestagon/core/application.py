import asyncio
import logging
from abc import abstractmethod, ABC

from typing import List, Tuple
from uuid import uuid4

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_store import AsyncEventStore
from bestagon.core.mapper import Mapper
from bestagon.core.message import DomainEvent
from bestagon.core.policy import ApplicationStreamNamePolicy
from bestagon.core.repository import AsyncRepository
from bestagon.core.subscription import AsyncApplicationSubscription


logger = logging.getLogger(__name__)


class EventProcessor(ABC):
    @property
    def name(self) -> str:
        return self.get_name()

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    async def process_event(self, event: DomainEvent) -> None:
        raise NotImplementedError


class Leader(EventProcessor):
    def __init__(self, event_store: AsyncEventStore, mapper: Mapper):
        self._event_store = event_store
        self._mapper = mapper

    @property
    def event_store(self) -> AsyncEventStore:
        return self._event_store

    @property
    def mapper(self) -> Mapper:
        return self._mapper

    async def create_application_subscription(self, subscription_id: str, start_position: int) -> AsyncApplicationSubscription:
        event_store_subscription = await self.event_store.create_stream_subscription(
            subscription_id=str(uuid4()),
            regex=f'{self.name}.*',
            start_position=start_position
        )
        app_subscription = AsyncApplicationSubscription(
            subscription_id=subscription_id,
            application_name=self.name,
            mapper=self.mapper,
            event_store_subscription=event_store_subscription
        )
        return app_subscription


class Follower(EventProcessor):
    def __init__(self, checkpoint_store: CheckpointStore):
        super().__init__()
        self._checkpoint_store = checkpoint_store
        self._subscriptions: List[AsyncApplicationSubscription] = list()
        self._tasks: List[asyncio.Task] = list()
        self._lock = asyncio.Lock()

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def subscriptions(self) -> Tuple[AsyncApplicationSubscription, ...]:
        return tuple(self._subscriptions)

    async def _consume_subscription(self, subscription: AsyncApplicationSubscription) -> None:
        checkpoint_name = self.create_checkpoint_name(follower_name=self.name, leader_name=subscription.application_name)
        while subscription.running:
            application_event = await subscription.next_event()
            await self.process_event(event=application_event.domain_event)
            await self.checkpoint_store.set_checkpoint(name=checkpoint_name, value=application_event.commit_position)

    @staticmethod
    def create_checkpoint_name(follower_name: str, leader_name: str) -> str:
        # TODO - here??? ORLY???
        return f'{follower_name}->{leader_name}'

    async def stop(self) -> None:
        for subscription in self._subscriptions:
            await subscription.stop()

        for task in self._tasks:
            task.cancel()

    async def subscribe_to(self, app: Leader) -> None:
        if not isinstance(app, Leader):
            raise TypeError(f'Invalid application type, expected <Leader>, got {type(app)}')

        async with self._lock:
            application_names = [subscription.application_name for subscription in self._subscriptions]
            if app.name in application_names:
                raise ValueError(f'Already subscribed to {app.name}')  # TODO - change exception type

            checkpoint_name = self.create_checkpoint_name(follower_name=self.name, leader_name=app.name)
            checkpoint = await self.checkpoint_store.get_checkpoint(name=checkpoint_name)
            subscription = await app.create_application_subscription(subscription_id=str(uuid4()), start_position=checkpoint)
            self._subscriptions.append(subscription)
            task = asyncio.create_task(self._consume_subscription(subscription))
            self._tasks.append(task)

            logger.info(f'{self.name} subscribed to application {app.name}')


class Application(Leader, Follower):
    def __init__(self, event_store: AsyncEventStore, checkpoint_store: CheckpointStore, mapper: Mapper):
        Leader.__init__(self, event_store=event_store, mapper=mapper)
        Follower.__init__(self, checkpoint_store=checkpoint_store)

        self._repository = AsyncRepository(
            event_store=event_store,
            stream_name_policy=ApplicationStreamNamePolicy(application_name=self.get_name()),  # TODO - hardcoded to simplify concept
            mapper=mapper
        )

    @property
    def repository(self) -> AsyncRepository:
        return self._repository


class Projection(Follower):
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError

    async def initialize(self) -> None:
        pass
