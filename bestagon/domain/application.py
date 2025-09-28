from abc import ABC, abstractmethod

from bestagon.adapters.event_store import EventStore
from bestagon.domain.domain_event import DomainEvent


class Application(ABC):
    def __init__(self, name: str, event_store: EventStore):
        self._name = name
        self._event_store = event_store

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def policy(self, domain_event: DomainEvent) -> None:
        raise NotImplementedError
