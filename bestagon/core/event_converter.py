from abc import ABC, abstractmethod

from bestagon.core.event_store import StoredEvent
from bestagon.domain.domain_event import DomainEvent


class EventConverter(ABC):
    @abstractmethod
    def from_stored_event(self, stored_event: StoredEvent) -> DomainEvent:
        raise NotImplementedError

    @abstractmethod
    def to_stored_event(self, domain_event: DomainEvent) -> StoredEvent:
        raise NotImplementedError
