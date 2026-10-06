from collections import defaultdict
from typing import Dict, Type, TYPE_CHECKING, List

from bestagon.core.exceptions import BestagonError
from bestagon.core.message import DomainEvent

if TYPE_CHECKING:
    from bestagon.core.aggregate import Aggregate


class AggregateTypeAlreadyRegisteredError(BestagonError):
    pass


class AggregateTypeNotRegisteredError(BestagonError):
    pass


class EventTypeAlreadyRegisteredError(BestagonError):
    pass


class EventTypeNotRegisteredError(BestagonError):
    pass


class AggregateTypeRegistry:
    # TODO - docs
    def __init__(self):
        self._aggregate_class_map: Dict[str, Type[Aggregate]] = dict()

    def get_aggregate_class(self, aggregate_type: str) -> Type[Aggregate]:
        if aggregate_type in self._aggregate_class_map:
            return self._aggregate_class_map[aggregate_type]
        raise AggregateTypeNotRegisteredError(
            f'Failed to retrieve aggregate class for aggregate type {aggregate_type}: '
            f'the aggregate type is not registered.'
        )

    def register_aggregate_type(self, aggregate_type: str, aggregate_class: Type[Aggregate]) -> None:
        if not issubclass(aggregate_class, Aggregate):
            raise TypeError(
                f'Failed to register aggregate type {aggregate_type}: '
                f'provided aggregate class should be a subclass of <Aggregate> class, got {aggregate_class}'
            )
        if not isinstance(aggregate_type, str):
            raise TypeError(
                f'Failed to register aggregate type {aggregate_type}: aggregate type should be <string>, got {type(aggregate_type)}'
            )
        if not aggregate_type:
            raise ValueError(
                f'Failed to register aggregate type {aggregate_type}: aggregate type should be a non empty string.'
            )
        if aggregate_type in self._aggregate_class_map:
            registered_class = self.get_aggregate_class(aggregate_type)
            raise AggregateTypeAlreadyRegisteredError(
                f'Failed to register aggregate type {aggregate_type}: '
                f'aggregate class {registered_class} already registered for this aggregate type.'
            )

        self._aggregate_class_map[aggregate_type] = aggregate_class


class EventTypeRegistry:
    # TODO - docs
    def __init__(self):
        # TODO - refactor, can be done using one dict
        self._event_class_map: Dict[str, Type[DomainEvent]] = dict()
        self._event_type_map: Dict[Type[DomainEvent], List[str]] = defaultdict(list)

    def get_event_class(self, event_type: str) -> Type[DomainEvent]:
        if event_type in self._event_class_map:
            return self._event_class_map[event_type]
        raise EventTypeNotRegisteredError(
            f'Failed to retrieve event class for event type {event_type}: '
            f'the event type is not registered.'
        )

    def get_event_types(self, event_class: Type[DomainEvent]) -> List[str]:
        event_types = self._event_type_map.get(event_class)
        if not event_types:
            raise EventTypeNotRegisteredError(
                f'Fialed to retrieve event types for event class {event_class.__class__.__qualname__}: '
                f'no event types registered for the event class'
            )
        return event_types

    def register_event_type(self, event_type: str, event_class: Type[DomainEvent]) -> None:
        if not issubclass(event_class, DomainEvent):
            raise TypeError(
                f'Failed to register event type {event_type}: '
                f'event class should be a subclass of {DomainEvent.__class__.__qualname__}, got {event_class}'
            )
        if not isinstance(event_type, str):
            raise TypeError(
                f'Failed to register event type {event_type}: '
                f'event type should be <string>, got {type(event_type)}'
            )
        if not event_type:
            raise TypeError(
                f'Failed to register event type {event_type}: '
                f'event type should be a non empty string'
            )
        if event_type in self._event_class_map:
            raise EventTypeAlreadyRegisteredError(
                f'Failed to register event type {event_type}: '
                f'the event type is already registered'
            )

        self._event_class_map[event_type] = event_class
        self._event_type_map[event_class].append(event_type)


aggregate_type_registry = AggregateTypeRegistry()
event_type_registry = EventTypeRegistry()


def register_aggregate_type(aggregate_type: str):
    def decorator(cls: Type[Aggregate]) -> Type[Aggregate]:
        aggregate_type_registry.register_aggregate_type(aggregate_type=aggregate_type, aggregate_class=cls)
        return cls
    return decorator


def register_event_type(event_type: str):
    def decorator(cls: Type[DomainEvent]) -> Type[DomainEvent]:
        event_type_registry.register_event_type(event_type=event_type, event_class=cls)
        return cls
    return decorator
