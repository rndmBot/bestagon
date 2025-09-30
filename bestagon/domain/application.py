from abc import ABC, abstractmethod
from typing import List, Tuple

from bestagon.domain.domain_event import DomainEvent


class Component(ABC):
    def __init__(self, name: str):
        self._name = name

    def __eq__(self, other):
        if isinstance(other, Application):
            return self.name == other.name
        return NotImplemented

    @property
    def name(self) -> str:
        return self._name


class Leader(Component):
    pass


class Follower(Component):
    def __init__(self, name: str):
        super().__init__(name=name)
        self._leaders: List[str] = list()

    @property
    def leaders(self) -> Tuple[str, ...]:
        return tuple(self._leaders)

    @abstractmethod
    def handle_event(self, domain_event: DomainEvent) -> None:
        raise NotImplementedError

    def follow(self, producer: Leader) -> None:
        if not isinstance(producer, Leader):
            raise TypeError(f'Invalid producer type {type(producer)}')

        if producer.name not in self.leaders:
            self._leaders.append(producer.name)


class Application(Leader, Follower):
    def __init__(self, name: str):
        super().__init__(name=name)

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def get_events(self, start_position: int, limit: int) -> List[DomainEvent]:
        # TODO - zapili
        raise NotImplementedError


class Projection(Follower):
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError
