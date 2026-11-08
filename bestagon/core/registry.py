from typing import Dict, Type, List, Callable

from bestagon.core.aggregate import Aggregate
from bestagon.core.exceptions import AggregateTypeNotRegisteredError, \
    EventTypeNotRegisteredError, EventTypeAlreadyRegisteredError, CommandHandlerAlreadyRegisteredError, \
    CommandHandlerNotRegisteredError, QueryHandlerNotRegisteredError, QueryHandlerAlreadyRegisteredError, \
    CanonicAggregateTypeNotRegisteredError, CanonicAggregateTypeAlreadyRegisteredError, \
    LegacyAggregateTypeAlreadyRegisteredError, AggregateTypeAlreadyRegisteredError, CanonicEventTypeNotRegistered, \
    CanonicEventTypeAlreadyRegisteredError, LegacyEventTypeAlreadyRegisteredError
from bestagon.core.message import DomainEvent, Command, Query


class AggregateTypeRegistry:
    def __init__(self):
        self._canonic_aggregate_type_map: Dict[Type[Aggregate], str] = dict()  # Only one canonic aggregate type can be registered for class
        self._inverse_canonic_map: Dict[str, Type[Aggregate]] = dict()
        self._legacy_aggregate_class_map: Dict[str, Type[Aggregate]] = dict()  # There can be multiple legacy types registered for the aggregate

    def get_canonic_aggregate_type(self, aggregate_class: Type[Aggregate]) -> str:
        if aggregate_class in self._canonic_aggregate_type_map:
            return self._canonic_aggregate_type_map[aggregate_class]
        raise CanonicAggregateTypeNotRegisteredError(
            f'Failed to retrieve canonic aggregate type for aggregate {aggregate_class}: '
            f'the canonic aggregate type is not registered.'
        )

    def get_aggregate_class(self, aggregate_type: str) -> Type[Aggregate]:
        if aggregate_type in self._inverse_canonic_map:
            return self._inverse_canonic_map[aggregate_type]
        elif aggregate_type in self._legacy_aggregate_class_map:
            return self._legacy_aggregate_class_map[aggregate_type]
        raise AggregateTypeNotRegisteredError(
            f'Failed to retrieve aggregate class for aggregate type {aggregate_type}: '
            f'the aggregate type is not registered.'
        )

    def register_canonic_aggregate_type(self, aggregate_type: str, aggregate_class: Type[Aggregate]) -> None:
        if not issubclass(aggregate_class, Aggregate):
            raise TypeError(
                f'Failed to register canonic aggregate type {aggregate_type}: '
                f'provided aggregate class should be a subclass of <Aggregate> class, got {aggregate_class}'
            )
        if not isinstance(aggregate_type, str):
            raise TypeError(
                f'Failed to register canonic aggregate type {aggregate_type}: aggregate type should be <string>, got {type(aggregate_type)}'
            )
        if not aggregate_type:
            raise ValueError(
                f'Failed to register canonic aggregate type {aggregate_type}: aggregate type should be a non empty string.'
            )
        if aggregate_class in self._canonic_aggregate_type_map:
            registered_type = self._canonic_aggregate_type_map[aggregate_class]
            raise CanonicAggregateTypeAlreadyRegisteredError(
                f'Failed to register canonic aggregate type {aggregate_type} for {aggregate_class}: '
                f'the canonic aggregate type {registered_type} already registered for the class.'
            )
        if aggregate_type in self._inverse_canonic_map:
            registered_class = self._inverse_canonic_map[aggregate_type]
            raise CanonicAggregateTypeAlreadyRegisteredError(
                f'Failed to register canonic aggregate type {aggregate_type} for {aggregate_class}: '
                f'the canonic aggregate type already registered for the {registered_class}'
            )
        # Canonic aggregate type cannot be registered if it already registered as legacy
        if aggregate_type in self._legacy_aggregate_class_map:
            raise AggregateTypeAlreadyRegisteredError(
                f'Failed to register canonic aggregate type {aggregate_type} for {aggregate_class}: '
                f'the canonic aggregate type already registered as legacy aggregate type'
            )

        self._canonic_aggregate_type_map[aggregate_class] = aggregate_type
        self._inverse_canonic_map[aggregate_type] = aggregate_class

    def register_legacy_aggregate_type(self, aggregate_type: str, aggregate_class: Type[Aggregate]) -> None:
        if not issubclass(aggregate_class, Aggregate):
            raise TypeError(
                f'Failed to register legacy aggregate type {aggregate_type}: '
                f'provided aggregate class should be a subclass of <Aggregate> class, got {aggregate_class}'
            )
        if not isinstance(aggregate_type, str):
            raise TypeError(
                f'Failed to register legacy aggregate type {aggregate_type}: aggregate type should be <string>, got {type(aggregate_type)}'
            )
        if not aggregate_type:
            raise ValueError(
                f'Failed to register legacy aggregate type {aggregate_type}: aggregate type should be a non empty string.'
            )

        if aggregate_type in self._legacy_aggregate_class_map:
            registered_class = self._legacy_aggregate_class_map[aggregate_type]
            raise LegacyAggregateTypeAlreadyRegisteredError(
                f'Failed to register legacy aggregate type {aggregate_type} for {aggregate_class}: '
                f'the legacy aggregate type already registered for the {registered_class} aggregate.'
            )

        # Legacy aggregate type cannot be registered if it already registered as canonic
        if aggregate_type in self._inverse_canonic_map:
            raise AggregateTypeAlreadyRegisteredError(
                f'Failed to register legacy aggregate type {aggregate_type} for {aggregate_class}: '
                f'the aggregate type already registered as canonic aggregate type'
            )

        self._legacy_aggregate_class_map[aggregate_type] = aggregate_class


class EventTypeRegistry:
    def __init__(self):
        self._canonic_event_type_map: Dict[Type[DomainEvent], str] = dict()
        self._inverse_canonic_map: Dict[str, Type[DomainEvent]] = dict()
        self._legacy_event_type_map: Dict[str, Type[DomainEvent]] = dict()

    def get_canonic_event_type(self, event_class: Type[DomainEvent]) -> str:
        if event_class in self._canonic_event_type_map:
            return self._canonic_event_type_map[event_class]
        raise CanonicEventTypeNotRegistered(
            f'Failed to retrieve canonic event type for event class {event_class.__qualname__}: '
            f'canonic event type is not registered'
        )

    def get_event_class(self, event_type: str) -> Type[DomainEvent]:
        if event_type in self._inverse_canonic_map:
            return self._inverse_canonic_map[event_type]
        elif event_type in self._legacy_event_type_map:
            return self._legacy_event_type_map[event_type]
        raise EventTypeNotRegisteredError(
            f'Failed to retrieve event class for event type {event_type}: '
            f'the event type is not registered.'
        )

    def get_event_types(self, event_class: Type[DomainEvent]) -> List[str]:
        event_types = [self.get_canonic_event_type(event_class)]
        for event_type, _class in self._legacy_event_type_map.items():
            if _class == event_class:
                event_types.append(event_type)
        return event_types

    def register_canonic_event_type(self, event_type: str, event_class: Type[DomainEvent]) -> None:
        if not issubclass(event_class, DomainEvent):
            raise TypeError(
                f'Failed to register canonic event type {event_type}: '
                f'event class should be a subclass of {DomainEvent.__qualname__}, got {event_class}'
            )
        if not isinstance(event_type, str):
            raise TypeError(
                f'Failed to register canonic event type {event_type}: '
                f'event type should be <string>, got {type(event_type)}'
            )
        if not event_type:
            raise ValueError(
                f'Failed to register canonic event type {event_type}: '
                f'event type should be a non empty string'
            )
        if event_class in self._canonic_event_type_map:
            raise CanonicEventTypeAlreadyRegisteredError(
                f'Failed to register canonic event type {event_type}: '
                f'the event type is already registered'
            )
        if event_type in self._inverse_canonic_map:
            raise CanonicEventTypeAlreadyRegisteredError(
                f'Failed to register canonic event type {event_type}: '
                f'the event type is already registered'
            )
        # Event type cannot be registered as canonic if it is already registered as legacy
        if event_type in self._legacy_event_type_map:
            raise EventTypeAlreadyRegisteredError(
                f'Failed to register canonic event type {event_type}: '
                f'the event type is already registered as legacy'
            )

        self._canonic_event_type_map[event_class] = event_type
        self._inverse_canonic_map[event_type] = event_class

    def register_legacy_event_type(self, event_type: str, event_class: Type[DomainEvent]) -> None:
        if not issubclass(event_class, DomainEvent):
            raise TypeError(
                f'Failed to register legacy event type {event_type}: '
                f'event class should be a subclass of {DomainEvent.__qualname__}, got {event_class}'
            )
        if not isinstance(event_type, str):
            raise TypeError(
                f'Failed to register legacy event type {event_type}: '
                f'event type should be <string>, got {type(event_type)}'
            )
        if not event_type:
            raise ValueError(
                f'Failed to register legacy event type {event_type}: '
                f'event type should be a non empty string'
            )
        # Event type cannot be registered as legacy if it already registered as canonic
        if event_type in self._inverse_canonic_map:
            raise EventTypeAlreadyRegisteredError(
                f'Failed to register legacy event type {event_type}: '
                f'the event type is already registered as canonic'
            )
        if event_type in self._legacy_event_type_map:
            raise LegacyEventTypeAlreadyRegisteredError(
                f'Failed to register legacy event type {event_type}: '
                f'the event type is already registered'
            )

        self._legacy_event_type_map[event_type] = event_class


class CommandHandlerRegistry:
    # TODO - there should be a mechanism of automatic registration of command handlers

    def __init__(self):
        self._command_handler_map: Dict[Type[Command], Callable] = dict()

    def get_command_handler(self, command_type: Type[Command]) -> Callable:
        if not command_type in self._command_handler_map:
            raise CommandHandlerNotRegisteredError(f'No command handler registered for command {command_type}')
        return self._command_handler_map[command_type]

    def register_command_handler(self, command_type: Type[Command], command_handler: Callable) -> None:
        if command_type in self._command_handler_map:
            raise CommandHandlerAlreadyRegisteredError(
                f'Failed to register command handler for command type {command_type}: '
                f'command handler for specified command type already registered.'
            )
        self._command_handler_map[command_type] = command_handler


class QueryHandlerRegistry:
    # TODO - there should be a mechanism of automatic query handlers registration

    def __init__(self):
        self._query_handler_map: Dict[Type[Query], Callable] = dict()

    def get_query_handler(self, query_type: Type[Query]) -> Callable:
        if query_type not in self._query_handler_map:
            raise QueryHandlerNotRegisteredError(f'No query handler registered for query {query_type}')
        return self._query_handler_map[query_type]

    def register_query_handler(self, query_type: Type[Query], query_handler: Callable) -> None:
        if query_type in self._query_handler_map:
            raise QueryHandlerAlreadyRegisteredError(f'Query handler already registered for query {query_type}')
        self._query_handler_map[query_type] = query_handler


aggregate_type_registry = AggregateTypeRegistry()
event_type_registry = EventTypeRegistry()
command_handler_registry = CommandHandlerRegistry()
query_handler_registry = QueryHandlerRegistry()


def register_aggregate_type(aggregate_type: str):
    """
    Registers canonic aggregate type for the aggregate.
    """
    def decorator(cls: Type[Aggregate]) -> Type[Aggregate]:
        aggregate_type_registry.register_canonic_aggregate_type(aggregate_type=aggregate_type, aggregate_class=cls)
        return cls
    return decorator


def register_event_type(event_type: str):
    """
    Registers canonic event type for the event.
    """
    def decorator(cls: Type[DomainEvent]) -> Type[DomainEvent]:
        event_type_registry.register_canonic_event_type(event_type=event_type, event_class=cls)
        return cls
    return decorator
