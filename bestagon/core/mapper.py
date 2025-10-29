import json
from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Type

from bestagon.core.aggregate import Aggregate
from bestagon.core.event_store import StreamEvent, NewStreamEvent
from bestagon.core.message import DomainEvent, DomainEventMetadata


class Mapper(ABC):
    @abstractmethod
    def get_aggregate_class(self, aggregate_type: str) -> Type[Aggregate]:
        raise NotImplementedError

    @abstractmethod
    def get_event_class(self, event_type: str) -> Type[DomainEvent]:
        raise NotImplementedError

    @abstractmethod
    def get_event_type(self, event_class: Type[DomainEvent]) -> str:
        raise NotImplementedError

    def to_domain_event(self, stream_event: StreamEvent) -> DomainEvent:
        event_class = self.get_event_class(event_type=stream_event.event_type)
        metadata_dict = json.loads(stream_event.metadata.decode())
        metadata = DomainEventMetadata(**metadata_dict)
        payload_dict = json.loads(stream_event.payload.decode())

        domain_event = event_class(metadata=metadata, **payload_dict)
        return domain_event

    def to_new_stream_event(self, domain_event: DomainEvent) -> NewStreamEvent:
        event_type = self.get_event_type(type(domain_event))
        payload = json.dumps(domain_event.get_payload()).encode()
        metadata = json.dumps(asdict(domain_event.metadata)).encode()

        new_stream_event = NewStreamEvent(
            stream_position=domain_event.metadata.aggregate_version,
            event_type=event_type,
            payload=payload,
            metadata=metadata
        )
        return new_stream_event
