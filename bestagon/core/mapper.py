from abc import ABC, abstractmethod
from typing import Type

from bestagon.core.aggregate import Aggregate
from bestagon.core.event_store import StreamEvent, NewStreamEvent
from bestagon.core.message import DomainEvent


class Mapper(ABC):
    @abstractmethod
    def get_aggregate_class(self, aggregate_type: str) -> Type[Aggregate]:
        raise NotImplementedError

    @abstractmethod
    def to_domain_event(self, stream_event: StreamEvent) -> DomainEvent:
        raise NotImplementedError

    @abstractmethod
    def to_new_stream_event(self, domain_event: DomainEvent) -> NewStreamEvent:
        raise NotImplementedError
