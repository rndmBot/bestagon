from abc import ABC, abstractmethod

from typing import List, Tuple

from bestagon.core.event_store import EventStore
from bestagon.core.mapper import Mapper
from bestagon.core.policy import ApplicationName, ProjectionName
from bestagon.domain.domain_event import DomainEvent


class Application:
    def __init__(self, name: ApplicationName, event_store: EventStore, mapper: Mapper):
        super().__init__()
        self._event_store = event_store
        self._mapper = mapper
        self._name = name
        self._leaders: List[ApplicationName] = list()

    @property
    def name(self) -> ApplicationName:
        return self._name

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def leaders(self) -> Tuple[ApplicationName, ...]:
        return tuple(self._leaders)

    @property
    def mapper(self) -> Mapper:
        return self._mapper

    def follow(self, application: 'Application') -> None:
        if not isinstance(application, Application):
            raise TypeError
        if application.name in self.leaders:
            return

        self._leaders.append(application.name)

    def get_events(self, start_position: int, limit: int) -> List[Tuple[DomainEvent, int]]:
        """Returns two-tuple (DomainEvent, CommitPosition)"""
        # TODO - rethink return type
        stream_events = self.event_store.get_events(regex_list=[self.name.to_regex()], start_position=start_position, limit=limit)

        domain_events = list()
        for stream_event in stream_events:
            domain_event = self.mapper.to_domain_event(stream_event=stream_event)
            two_tuple = (domain_event, stream_event.commit_position)
            domain_events.append(two_tuple)

        return domain_events

    @abstractmethod
    def process_event(self, event: DomainEvent) -> None:
        raise NotImplementedError


class Projection:
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    def __init__(self, name: ProjectionName):
        super().__init__()
        self._name = name
        self._leaders: List[ApplicationName] = list()

    @property
    def leaders(self) -> Tuple[ApplicationName, ...]:
        return tuple(self._leaders)

    @property
    def name(self) -> ProjectionName:
        return self._name

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError

    def follow(self, application: 'Application') -> None:
        if not isinstance(application, Application):
            raise TypeError
        if application.name in self.leaders:
            return

        self._leaders.append(application.name)

    @abstractmethod
    def process_event(self, event: DomainEvent) -> None:
        raise NotImplementedError
