from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Dict, Callable, Type

from bestagon.core.exceptions import AggregateTypeNotRegisteredError, EventHandlerNotRegisteredError
from bestagon.core.message import DomainEvent, DomainMessage


# TODO - future improvements:
#   1. Improve event handler registration, ideally with @event_handler decorator
#   2. The inheritance mechanism should be reviewed: how exactly children should inherit events and reregister types?


@dataclass(frozen=True)
class PendingEvent:
    """
    The pending event represents the new event that already was triggered, but still not persisted in the event store.
    It contains the original `DomainEvent` and `aggregate_version`. This `aggregate_version` is required for event persistence
    because it should be propagated to `DomainEventMetadata` and the aggregate is the one and only source of this parameter.
    """
    aggregate_version: int
    domain_event: DomainEvent


class Aggregate(ABC):
    # TODO - docs

    _AGGREGATE_TYPE = None
    INITIAL_VERSION = 0

    @dataclass(frozen=True)
    class AggregateCreated(DomainEvent):
        """
        This class must be subclassed for every event that leads to the creation of the aggregate.
        For example `CustomerRegistered`, `TicketOpened`, `MerchantOnboarded`.
        """
        pass

    @dataclass(frozen=True)
    class AggrgateEvent(DomainEvent):
        """
        This class must be subclassed for every event after the `AggregateCreated` event.
        """
        pass

    def __init__(self, event: AggregateCreated, aggregate_id: str, aggregate_version: int):
        self._aggregate_id = aggregate_id
        self._aggregate_version = aggregate_version
        self._original_version = aggregate_version

        self._pending_events: List[PendingEvent] = list()

    def __init_subclass__(cls, **kwargs):
        """
        In your domain you can easily have aggregates that inherit other aggregates, for example: Customer -> VIPCustomer.
        Such inheritance creates a dangerous situation when child inherits parent's aggregate type and, if child
        have not registered its own aggregate type using `register_aggregate_type` decorator, then parent's aggregate
        type will be used on serialization step which can make deserialization and reconstruction not possible.
        To protect aggregate from this the method resets _AGGREGATE_TYPE to None, it is executed before the decorator,
        so child aggregates without decorator will have _AGGREGATE_TYPE == None and it will raise an exception
        on serialization state instead of silently corrupting the data.
        """
        super().__init_subclass__(**kwargs)
        cls._AGGREGATE_TYPE = None

    @property
    def aggregate_id(self) -> str:
        return self._aggregate_id

    @property
    def aggregate_type(self) -> str:
        if not self._AGGREGATE_TYPE:
            raise AggregateTypeNotRegisteredError(
                f'No aggregate type is registered for aggregate {self.__class__.__qualname__}, '
                f'register the aggregate type with `register_aggregate_type` decorator to fix this problem, '
                f'for example:\n'
                f'@register_aggregate_type("customer")\n'
                f'class Customer(Aggregate):\n'
                f'  ....'
            )
        return self._AGGREGATE_TYPE

    @property
    def aggregate_version(self) -> int:
        return self._aggregate_version

    @property
    def original_version(self) -> int:
        return self._original_version

    @property
    def pending_events(self) -> Tuple[PendingEvent, ...]:
        return tuple(self._pending_events)

    def apply_event(self, event: 'Aggregate.AggrgateEvent') -> None:
        event_handler_routing = self.get_event_handler_routing()
        handler = event_handler_routing.get(type(event))
        if handler is not None:
            handler(event)
        else:
            raise EventHandlerNotRegisteredError(
                f'Failed to apply event {type(event)} to aggregate "{self.__class__.__qualname__}": '
                f'no event handler provided for the event, please check `get_event_handler_routing` method, it should contain '
                f'the routing to valid event handler, for example {{self.CustomerCreated: self._when_customer_created}}'
            )

    def clear_pending_events(self) -> None:
        """Clears all pending events on the Aggregate"""
        self._pending_events.clear()

    @classmethod
    def create_aggregate(cls, event: 'AggregateCreated', aggregate_id: str) -> 'Aggregate':
        """
        This method creates an instance of aggregate class.
        When you need to create a new instance of the aggregate you do not instantiate it directly, instead you need to
        define a factory method on your aggregate class. This factory method should implement the following workflow:
            1. Validate the input
            2. Execute business logic if necessary
            3. Create <AggregateCreated> event
            4. Instantiate aggregate using `create_aggregate` method
            5. Return newly created aggregate.

        For example:
            class Customer(Aggregate):
                class Created(AgregateCreated):
                    # Fields omited
                    ...

                @classmethod
                def create(cls, customer_id: str) -> 'Customer':
                    # Validate input
                    # Execute business logic if necessary

                    event = cls.Created(cutomer_id=customer_id)
                    aggregate_id = cls.create_id(customer_id)
                    obj = cls.create_aggregate(
                        event=event,
                        aggregate_id=aggregate_id,
                        aggregate_version=cls.INITIAL_VERSION
                    )
                    return obj
        """
        if not isinstance(event, cls.AggregateCreated):
            raise TypeError(
                f'Failed to create aggregate {cls.__qualname__}: '
                f'the event must be a subclass of <Aggregate.AggregateCreated> class'
            )

        obj = cls(event=event, aggregate_id=aggregate_id, aggregate_version=cls.INITIAL_VERSION)
        pending_event = PendingEvent(
            aggregate_version=cls.INITIAL_VERSION,
            domain_event=event
        )
        obj._pending_events.append(pending_event)
        obj._original_version = None
        return obj

    @staticmethod
    @abstractmethod
    def create_id(*args, **kwargs) -> str:
        """
        Reimplement to create ID of the aggregate.
        Please note that aggregate ID MUST BE GLOBALLY UNIQUE and no other aggregate accross the service should not
        share the same aggregate_id.
        """
        raise NotImplementedError

    @abstractmethod
    def get_event_handler_routing(self) -> Dict[Type['Aggregate.AggrgateEvent'], Callable]:
        """
        This class have to be reimplemented to provide the routing from event to the corresponding event handler,
        for example:

            def event_handler_routing(self):
                routing = {
                    self.NameChanged: self._when_name_changed,
                    self.Blocked: self._when_blocked
                }
                return routing

        TODO - future improvements:
            Event handler registration process is still manual and should be improved, ideally there should be
            an @event_handler decorator which will register event handler automatically
        """
        raise NotImplementedError

    def mutate(self, message: DomainMessage) -> None:
        """
        The method is used by framework on aggregate reconstruction stage and should not be called directly.
        """
        if not isinstance(message.event, self.AggrgateEvent):
            raise TypeError(
                f'Failed to mutate aggregate {self.__class__.__qualname__} with event {message.event.__class__.__qualname__}: '
                f'the event must be a subclass of <Aggregate.AggrgateEvent> class'
            )
        self.apply_event(event=message.event)
        self._aggregate_version = message.metadata.aggregate_version
        self._original_version = message.metadata.aggregate_version

    def trigger_event(self, event: DomainEvent) -> None:
        if not isinstance(event, self.AggrgateEvent):
            raise TypeError(
                f'Failed to trigger event {event.__class__.__qualname__}: '
                f'the event must be a subclass of <Aggregate.AggrgateEvent> class'
            )

        self.apply_event(event)
        self._aggregate_version = self.aggregate_version + 1
        pending_event = PendingEvent(
            aggregate_version=self.aggregate_version,
            domain_event=event
        )
        self._pending_events.append(pending_event)
