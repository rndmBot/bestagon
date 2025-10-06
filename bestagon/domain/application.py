from abc import ABC, abstractmethod

from typing import List, Tuple

from bestagon.core.event_store import EventStore
from bestagon.core.mapper import Mapper
from bestagon.core.policy import ApplicationName
from bestagon.domain.domain_event import DomainEvent


class Component(ABC):
    @property
    def name(self) -> str:
        return self.get_name()

    @abstractmethod
    def process_event(self, event: DomainEvent) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError


class Leader(Component):
    pass


class Follower(Component):
    def __init__(self):
        self._leaders: List[str] = list()

    @property
    def leaders(self) -> Tuple[str, ...]:
        return tuple(self._leaders)

    def follow(self, leader: Leader) -> None:
        if not isinstance(leader, Leader):
            raise TypeError(f'Failed to follow leader, expected <Leader> class, got {type(leader)}')
        if leader.name not in self.leaders:
            self._leaders.append(leader.name)


class Application(Follower, Leader):
    def __init__(self, application_name: ApplicationName, event_store: EventStore, mapper: Mapper):
        super().__init__()
        self._event_store = event_store
        self._mapper = mapper
        self._application_name = application_name

    @property
    def application_name(self) -> ApplicationName:
        return self._application_name

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def mapper(self) -> Mapper:
        return self._mapper

    def get_name(self) -> str:
        return self.application_name.to_string()

    def get_events(self, start_position: int, limit: int) -> List[Tuple[DomainEvent, int]]:
        """Returns two-tuple (DomainEvent, CommitPosition)"""
        # TODO - rethink return type
        stream_events = self.event_store.get_events(regex_list=[self.application_name.to_regex()], start_position=start_position, limit=limit)

        domain_events = list()
        for stream_event in stream_events:
            domain_event = self.mapper.to_domain_event(stream_event=stream_event)
            two_tuple = (domain_event, stream_event.commit_position)
            domain_events.append(two_tuple)

        return domain_events


class Projection(Follower):
    # TODO - name as an object (ProjectionName)
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    def __init__(self, name: str):
        super().__init__()
        self._name = name

    def __eq__(self, other):
        if isinstance(other, Application):
            return self.name == other.name
        return NotImplemented

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError

    def get_name(self) -> str:
        return self._name
