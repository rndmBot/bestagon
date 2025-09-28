from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Tuple

from bestagon.adapters.event_store import EventStore
from bestagon.adapters.stored_event import DomainEvent, ApplicationEvent
from bestagon.adapters.repository import EventSourcedRepository

if TYPE_CHECKING:
    from bestagon.domain.aggregate import Aggregate


# TODO - implement Signal class


class Component(ABC):
    """Component of a System"""

    def __init__(self, name: str):
        self._name = name

    def __eq__(self, other):
        if isinstance(other, Component):
            return self.name == other.name
        return NotImplemented

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def policy(self, domain_event: DomainEvent) -> None:
        raise NotImplementedError


class Projection(Component):
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError


class Application(Component):
    def __init__(self, name: str, event_store: EventStore):
        super().__init__(name=name)
        self._leaders = list()
        self._event_store = event_store
        self._repository = EventSourcedRepository(
            event_store=event_store,
            stream_name_prefix=self.name
        )

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def leaders(self) -> Tuple[str, ...]:
        # TODO - to component
        # TODO - rethink
        return tuple(self._leaders)

    @property
    def repository(self) -> EventSourcedRepository:
        return self._repository

    def get_application_events(self, start_position: int, limit: int) -> List[ApplicationEvent]:
        # TODO - rethink

        """Returns events from application sequence"""
        events = self.event_store.get_application_events(
            application_name=self.name,
            start=start_position,
            limit=limit,
            # TODO - separator???
        )
        return events

    def save(self, aggregate: 'Aggregate') -> None:
        self.repository.save(aggregate)
        # TODO - notify system about changes

    def subscribe_to(self, application: 'Application') -> None:
        # TODO - to component
        # TODO - rethink

        if application.name in self.leaders:
            return
        self._leaders.append(application.name)
