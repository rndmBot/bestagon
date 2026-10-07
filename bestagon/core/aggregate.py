from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple

from bestagon.core.exceptions import BestagonError
from bestagon.core.message import DomainEvent


class DomainException(BestagonError):
    """Should be raised in case of business rules violation"""
    pass


class AggregateIDMismatch(BestagonError):
    pass


class AggregateVersionError(BestagonError):
    pass


class NoEventhandlerError(BestagonError):
    """Raised if no event handler is registered for the event."""
    pass


def event_handler(event_type):
    def decorator(func):
        setattr(func, '_event_type', event_type)
        return func
    return decorator


class Aggregate(ABC):
    """
    A base class for event-sourced aggregate. This class should not be instantiated directly, instead it should be sublcassed
    to represent a specific business entity.

    Event-sourced aggregate serves as a representation of an important business entity in the business domain, for example
    Merchant, Customer, Ticket and so on. The aggregate is a source of domain events, every time something hoppens in the aggregate
    it is recorded as an event. These events change the agggregte state and they are persisted in event store in chronological
    sequence.

    Aggregate lifecycle.
    Aggregate starts it's lifecycle from `Aggregate.Created` event. This event explicitly tells that aggregate now exists and sets
    the initial state of the aggregate. Every consequent event must subclass `Aggregate.Event` event. These events capture the important
    business change and directly change the aggregate state using `apply` method.

    Aggregate subclassing:
    1. Define `Created` event for the aggregate. It is the event from which the aggregate starts it's lifecycle and which
       sets the initial state of he aggregate.

    2. Reimplement `__init__` method. It must accept only one parameter - `Created` event defined on the first step. Use fields
       from the event to set the initial state of the aggregate inside the `__init__`

    3. Reimplement `create_id` method - each aggregate should have it's own logic to create a unique ID for each aggregate.
       IMPORTANT - the ID, returned by the `create_id` method must be globally unique across the service.

    4. Reimpement `get_aggregate_type` method. It must provide globally unique string that will be saved in
       domain event metadata and used on the aggregate reconstruction stage. Once defined, it is not recommended to
       change this value during the whole system lifecycle.

    5. Implement the factory class method that creates the aggregate, it's name must reflect your business domain,
       for example if Customer starts it's lifecycle from registration on a web site then factory method
       of Customer aggregate should be named `register`. nside this method create `Created` event, then use `create_aggregate`
       method to initialize the aggregate and return it.

    6. Implement every other event for the aggregate by subclassing `Aggregate.Event` class.

    7. For each event there should be a corresponding method that first executes business logic, the aptures the changes
       by creating the event and triggers it by calling `trigger_event` method.

    8. After event triggered it should change the aggregate state, to make it happen provide the necessary logic inside
       `apply_event` method.
    """
    INITIAL_VERSION = 0

    @dataclass(frozen=True)
    class Created(DomainEvent):
        """
        This class must be subclassed for every event that leads to the creation of the aggregate.
        For example `CustomerRegistered`, `TicketOpened`, `MerchantOnboarded`.
        """
        pass

    @dataclass(frozen=True)
    class Event(DomainEvent):
        """
        This class must be subclassed for every event after the `Created` event.
        """
        pass

    def __init__(self, event: Created, aggregate_id: str, aggregate_version: int):
        self._aggregate_id = aggregate_id
        self._aggregate_version = aggregate_version

        self._pending_events: List[DomainEvent] = list()
        self._event_handler_map = dict()
        self._register_event_handlers()

    @property
    def aggregate_id(self) -> str:
        return self._aggregate_id

    @property
    def aggregate_version(self) -> int:
        return self._aggregate_version

    @property
    def pending_events(self) -> Tuple[DomainEvent, ...]:
        return tuple(self._pending_events)

    def _register_event_handlers(self) -> None:
        # TODO - docstring
        cls = type(self)
        for name in dir(cls):
            attr = getattr(cls, name)
            event_type = getattr(attr, '_event_type', None)
            if event_type is not None:
                self._event_handler_map[event_type] = getattr(self, name)

    def apply_event(self, event: DomainEvent) -> None:
        # TODO - docstring
        handler = self._event_handler_map.get(type(event))
        if handler is not None:
            handler(event)
        else:
            # TODO - rewrite error message, mention to decorate handlers with "@event_handler"
            raise NoEventhandlerError(
                f'Failed to apply event {type(event)} to aggregate "{self.__class__.__qualname__}" - "{self.aggregate_id}": '
                f'no event handler provided for the event, please check `get_event_routing` method, it should contain '
                f'the routing to valid event handler, for example {{self.CustomerCreated: self._when_customer_created}}'
            )

    def clear_pending_events(self) -> None:
        """Clears all pending events on the Aggregate"""
        self._pending_events.clear()

    @classmethod
    def create_aggregate(cls, event: 'Created', aggregate_id: str, aggregate_version: int) -> 'Aggregate':
        """Actually creates new aggregate. Should be used by factory method implemented on specific aggregate instance."""
        # TODO - validate that the event is "Created" class
        if aggregate_version != cls.INITIAL_VERSION:
            raise AggregateVersionError(
                f'Failed to create aggregate "{cls.__class__.__qualname__}" - '
                f'expected aggregate version {cls.INITIAL_VERSION}, got {aggregate_version}'
            )

        obj = cls(event=event, aggregate_id=aggregate_id, aggregate_version=aggregate_version)
        obj._pending_events.append(event)
        return obj

    @staticmethod
    @abstractmethod
    def create_id(*args, **kwargs) -> str:
        """
        Reimplement to create ID of the aggregate.
        Aggregate ID MUST BE UNIQUE accross the same aggregate type.
        """
        raise NotImplementedError

    def mutate(self, event: DomainEvent) -> None:
        self.apply_event(event)
        self._aggregate_version = self.aggregate_version + 1

    def trigger_event(self, event: DomainEvent) -> None:
        self.mutate(event=event)
        self._pending_events.append(event)
