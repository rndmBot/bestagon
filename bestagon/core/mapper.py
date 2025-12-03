import json
from dataclasses import asdict
from typing import Type, Dict

from bestagon.core.aggregate import Aggregate
from bestagon.core.event_store import StreamEvent, NewStreamEvent
from bestagon.core.message import DomainEvent, DomainEventMetadata
from bestagon.exceptions import TypeNotRegisteredError, TypeAlreadyRegisteredError


class Mapper:
    # TODO - ORLY - add possibility to pass custom serializer???
    # TODO - how to register Aggregate and Event types automatically

    def __init__(self):
        self._aggregate_class_map: Dict[str, Type[Aggregate]] = dict()

        self._event_class_map: Dict[str, Type[DomainEvent]] = dict()
        self._event_type_map: Dict[Type[DomainEvent], str] = dict()

    def get_aggregate_class(self, aggregate_type: str) -> Type[Aggregate]:
        if aggregate_type in self._aggregate_class_map:
            return self._aggregate_class_map[aggregate_type]
        raise TypeNotRegisteredError(f'No aggregate class registered for aggregate type: {aggregate_type}')

    def get_event_class(self, event_type: str) -> Type[DomainEvent]:
        if event_type in self._event_class_map:
            return self._event_class_map[event_type]
        raise TypeNotRegisteredError(f'No event class registered for event type: {event_type}')

    def get_event_type(self, event_class: Type[DomainEvent]) -> str:
        if event_class in self._event_type_map:
            return self._event_type_map[event_class]
        raise TypeNotRegisteredError(f'No event type registered for event class: {event_class}')

    def register_aggregate_type(self, aggregate_class: Type[Aggregate], aggregate_type: str) -> None:
        if not issubclass(aggregate_class, Aggregate):
            raise TypeError(f'Invalid aggregate class type, expected <Aggregate>, got {aggregate_class}')
        if not isinstance(aggregate_type, str):
            raise TypeError('Aggregate class should be string')

        if aggregate_type in self._aggregate_class_map:
            raise TypeAlreadyRegisteredError(f'Type {aggregate_type} already registered for aggregate class {self._aggregate_class_map[aggregate_type]}')
        self._aggregate_class_map[aggregate_type] = aggregate_class

    def register_event_type(self, event_class: Type[DomainEvent], event_type: str) -> None:
        if not issubclass(event_class, DomainEvent):
            raise TypeError(f'Invalid event class type, expected <DomainEvent>, got {event_class}')
        if not isinstance(event_type, str):
            raise TypeError('Event type should be string')

        if event_type in self._event_class_map:
            raise TypeAlreadyRegisteredError(f'Type {event_type} already registered for event {self._event_class_map[event_type]}')
        self._event_class_map[event_type] = event_class
        self._event_type_map[event_class] = event_type

    def to_domain_event(self, stream_event: StreamEvent) -> DomainEvent:
        # TODO - Metadata class is hardcoded, rethink the concept - is there a possibility to use another class for metadata???
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


mapper = Mapper()
