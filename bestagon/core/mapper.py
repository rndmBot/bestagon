import inspect
import json
from dataclasses import asdict
from typing import Type, Dict, Callable

from bestagon.core.aggregate import Aggregate
from bestagon.core.message import DomainEvent, DomainEventMetadata, NewStreamEvent, StreamEvent, Query, Command
from bestagon.exceptions import TypeNotRegisteredError, TypeAlreadyRegisteredError, HandlerAlreadyRegistered


class Mapper:
    # TODO - ORLY - add possibility to pass custom serializer???

    def __init__(self):
        self._aggregate_class_map: Dict[str, Type[Aggregate]] = dict()

        self._event_class_map: Dict[str, Type[DomainEvent]] = dict()
        self._event_type_map: Dict[Type[DomainEvent], str] = dict()
        self._query_handler_map: Dict[Type[Query], Callable] = dict()
        self._command_handler_map: Dict[Type[Command], Callable] = dict()

    def get_aggregate_class(self, aggregate_type: str) -> Type[Aggregate]:
        if aggregate_type in self._aggregate_class_map:
            return self._aggregate_class_map[aggregate_type]
        raise TypeNotRegisteredError(f'No aggregate class registered for aggregate type: {aggregate_type}')

    def get_command_handler(self, command_type: Type[Command]) -> Callable:
        if command_type not in self._command_handler_map:
            raise TypeNotRegisteredError(f'No command handler registered for command {command_type}')
        return self._command_handler_map[command_type]

    def get_event_class(self, event_type: str) -> Type[DomainEvent]:
        if event_type in self._event_class_map:
            return self._event_class_map[event_type]
        raise TypeNotRegisteredError(f'No event class registered for event type: {event_type}')

    def get_event_type(self, event_class: Type[DomainEvent]) -> str:
        if event_class in self._event_type_map:
            return self._event_type_map[event_class]
        raise TypeNotRegisteredError(f'No event type registered for event class: {event_class}')

    def get_query_handler(self, query_type: Type[Query]) -> Callable:
        if query_type not in self._query_handler_map:
            raise TypeNotRegisteredError(f'No query handler registered for query {query_type}')
        return self._query_handler_map[query_type]

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

    def register_command_handler(self, command_type: Type[Command], handler: Callable) -> None:
        if command_type in self._command_handler_map:
            raise HandlerAlreadyRegistered(f'Command handler already registered for command {command_type}')
        if not issubclass(command_type, Command):
            raise TypeError(f'Invalid command type {command_type}')
        self._command_handler_map[command_type] = handler

    def register_query_handler(self, query_type: Type[Query], handler: Callable) -> None:
        if query_type in self._query_handler_map:
            raise TypeError(f'Handler for query {query_type} is already registered')
        if not issubclass(query_type, Query):
            raise TypeError(f'Invalid query type {query_type}')
        self._query_handler_map[query_type] = handler

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


def _extract_type(fn: Callable) -> Type:
    signature = inspect.signature(fn)
    params = list(signature.parameters.values())
    if params:
        extracted_type = params[0].annotation
        return extracted_type
    else:
        raise ValueError('No parameters found in signature')


def command_handler():
    def decorator(fn):
        command_type = _extract_type(fn)
        if not issubclass(command_type, Command):
            raise TypeError(f'Invalid command type {command_type}')
        mapper.register_command_handler(command_type=command_type, handler=fn)
        return fn
    return decorator


def query_handler():
    def decorator(fn):
        query_type = _extract_type(fn)
        if not issubclass(query_type, Query):
            raise TypeError(f'Invalid query type {query_type}')
        mapper.register_query_handler(query_type=query_type, handler=fn)
        return fn
    return decorator


def register_aggregate_type():
    def decorator(cls: Type[Aggregate]) -> Type[Aggregate]:
        mapper.register_aggregate_type(aggregate_class=cls, aggregate_type=cls.get_aggregate_type())
        return cls
    return decorator


def register_event_type(event_type: str):
    def decorator(cls: Type[DomainEvent]) -> Type[DomainEvent]:
        mapper.register_event_type(event_class=cls, event_type=event_type)
        return cls
    return decorator
