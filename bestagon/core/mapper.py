from abc import ABC, abstractmethod
from typing import Type

from bestagon.core.event_store import StreamEvent, NewStreamEvent
from bestagon.domain.aggregate import Aggregate
from bestagon.domain.domain_event import DomainEvent


class Mapper(ABC):
    # TODO - IDEA - add TypeMap singleton class that will hold global map of event types

    @abstractmethod
    def get_aggregate_class(self, aggregate_type: str) -> Type[Aggregate]:
        raise NotImplementedError

    @abstractmethod
    def to_domain_event(self, stream_event: StreamEvent) -> DomainEvent:
        raise NotImplementedError

    @abstractmethod
    def to_new_stream_event(self, domain_event: DomainEvent) -> NewStreamEvent:
        raise NotImplementedError
