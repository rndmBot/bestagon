import json
from typing import Type, Dict, Callable

from bestagon.core.aggregate import DomainEvent, DomainEventMetadata
from bestagon.core.event_store import EventStoreEvent, NewEventStoreEvent
from bestagon.core.exceptions import TypeNotRegisteredError, HandlerAlreadyRegistered
from bestagon.core.message import Query, Command


class Mapper:
    """
    There can be multiple event types for a single event. This feature is added to fix the situation when there are several event types in event store
    for the same event because of technical error or mistake, in such case the event can be successfully reconstructed.
    When event with multiple types is serialized, the first registered type will be used as an event type.
    """

    def __init__(self):
        self._query_handler_map: Dict[Type[Query], Callable] = dict()
        self._command_handler_map: Dict[Type[Command], Callable] = dict()

    def get_command_handler(self, command_type: Type[Command]) -> Callable:
        if command_type not in self._command_handler_map:
            raise TypeNotRegisteredError(f'No command handler registered for command {command_type}')
        return self._command_handler_map[command_type]

    def get_query_handler(self, query_type: Type[Query]) -> Callable:
        if query_type not in self._query_handler_map:
            raise TypeNotRegisteredError(f'No query handler registered for query {query_type}')
        return self._query_handler_map[query_type]

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

    def to_domain_event(self, stream_event: EventStoreEvent) -> DomainEvent:
        event_class = self.get_event_class(event_type=stream_event.event_type)
        metadata_dict = json.loads(stream_event.metadata.decode())
        metadata = DomainEventMetadata.from_dict(metadata_dict)
        payload_dict = json.loads(stream_event.payload.decode())

        domain_event = event_class(metadata=metadata, **payload_dict)
        return domain_event

    def to_new_event_store_event(self, domain_event: DomainEvent) -> NewEventStoreEvent:
        event_type = self.get_event_type(type(domain_event))
        payload = json.dumps(domain_event.get_payload()).encode()
        metadata = json.dumps(domain_event.metadata.to_dict()).encode()

        new_stream_event = NewEventStoreEvent(
            event_type=event_type,
            payload=payload,
            metadata=metadata
        )
        return new_stream_event


# TODO - to many modules depend on this class, how to reduce this dependncy?
mapper = Mapper()
