import json
from typing import Type, Dict, Callable

from bestagon.core.event_store import EventStoreEvent, NewEventStoreEvent
from bestagon.core.exceptions import TypeNotRegisteredError, HandlerAlreadyRegistered
from bestagon.core.message import Query, Command, DomainMessage
from bestagon.core.registry import event_type_registry


class Mapper:
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


