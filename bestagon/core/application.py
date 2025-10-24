from abc import abstractmethod, ABC

from typing import List, Tuple
from uuid import uuid4

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_store import AsyncEventStore
from bestagon.core.mapper import Mapper
from bestagon.core.policy import ApplicationStreamNamePolicy
from bestagon.core.repository import EventSourcedRepository
from bestagon.core.subscription import AsyncApplicationSubscription
from bestagon.domain.domain_event import DomainEvent


class EventProcessor(ABC):
    @property
    def name(self) -> str:
        return self.get_name()

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def process_event(self, event: DomainEvent) -> None:
        raise NotImplementedError


class Leader(EventProcessor):
    pass


class Follower(EventProcessor):
    def __init__(self):
        super().__init__()
        self._leaders: List[str] = list()

    @property
    def leaders(self) -> Tuple[str, ...]:
        return tuple(self._leaders)

    def follow(self, application: 'Application') -> None:
        if not isinstance(application, Application):
            raise TypeError
        if application.name in self.leaders:
            return

        self._leaders.append(application.name)


class Application(Leader, Follower):
    def __init__(self, event_store: AsyncEventStore, checkpoint_store: CheckpointStore, mapper: Mapper):
        super().__init__()
        self._event_store = event_store
        self._checkpoint_store = checkpoint_store  # TODO - Projection has it also
        self._mapper = mapper
        self._repository = EventSourcedRepository(
            event_store=event_store,
            stream_name_policy=ApplicationStreamNamePolicy(application_name=self.get_name()),  # TODO - hardcoded to simplify concept
            mapper=mapper
        )

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def event_store(self) -> AsyncEventStore:
        return self._event_store

    @property
    def mapper(self) -> Mapper:
        return self._mapper

    @property
    def repository(self) -> EventSourcedRepository:
        return self._repository

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

    # TODO - subscribe to application (should be also in Projection)


class Projection(Follower):
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError
