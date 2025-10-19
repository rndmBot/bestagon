from abc import abstractmethod, ABC

from typing import List, Tuple

from bestagon.core.event_store import EventStore
from bestagon.core.mapper import Mapper
from bestagon.core.policy import ApplicationStreamNamePolicy
from bestagon.core.repository import EventSourcedRepository
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
    def __init__(self, event_store: EventStore, mapper: Mapper):
        super().__init__()
        self._event_store = event_store
        self._mapper = mapper
        self._repository = EventSourcedRepository(
            event_store=event_store,
            stream_name_policy=ApplicationStreamNamePolicy(application_name=self.get_name()),
            mapper=mapper
        )

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def mapper(self) -> Mapper:
        return self._mapper

    @property
    def repository(self) -> EventSourcedRepository:
        return self._repository

    def get_events(self, start_position: int, limit: int) -> List[Tuple[DomainEvent, int]]:
        """Returns two-tuple (DomainEvent, CommitPosition)"""
        # TODO - rethink return type
        # TODO - ACHTUNG - redesign and refactor
        stream_events = self.event_store.get_events(regex_list=[f'.*{self.name}.*'], start_position=start_position, limit=limit)

        domain_events = list()
        for stream_event in stream_events:
            domain_event = self.mapper.to_domain_event(stream_event=stream_event)
            two_tuple = (domain_event, stream_event.commit_position)
            domain_events.append(two_tuple)

        return domain_events


class Projection(Follower):
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError
