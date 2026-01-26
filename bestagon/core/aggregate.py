from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

from bestagon.core.message import DomainEvent, Created
from bestagon.exceptions import AggregateIDMismatch, AggregateVersionError


class Aggregate(ABC):
    """
    According to Eric Evans book Domain Driven Design: Tackling Complexity in the Heart of Software:
        An AGGREGATE is a cluster of associated objects that we treat as a unit for the purpose of data changes.

    The class is an implementation of an Event Sourced Aggregate.
    Each event sourced aggregate contains a set of predefined domain events, which discovered during modelling stage using event storming or other knowledge crunching method.

    # Aggregate creation:
    Event sourced aggregate starts its life cycle from 'Created' event. The only right way to do it currenty is to define factory method on the aggregate, this method
    should create necessary event and then use '_create' method to instantiate aggregate.

    For example, imagine you have a ShoppingCart aggregate, according to ubiquitious language of your domain it is 'created', so you can create a factory method, called 'create'
    for the specified aggregate:

        @classmethod
        def create(cls, param_1, param_2) -> 'ShoppingCart':
            metadata = DomainEventMetadata()  # Details omitted for simplicity
            event = cls.Created(metadata, param_1, param_2)
            obj = cls._create(event)
            return obj

    # Consequtive events:
    In addition to 'Created' event, aggregate can have 0 or more events that can happen after creation of aggregate. Each of these events change aggregate state in some way.
    For example, if we talk about shopping cart, then we can 'add_item' to it, so there will be a method on aggregate:

        def add_item(command: AddShoppingCartItem) -> None:
            # First, you perform validation of input parameters (whether it is a command, entity or just a set of parameters)
            self.validate_command(command)

            # Second, you execute business logic to figure out what have happened
            # Business logic code in here

            # Third - you create an Event which records what have happened
            event = self.ItemAdded(
                metadata=metadata,
                item_name=command.item_name
            )

            # Finally - you trigger event to launch change of state process
            sef.trigger_event(event)

    When you trigger event, the aggregate calls 'mutate' method which validates the event and passes it to 'apply_event' method.
    This method must know how to change the state of the aggregate and this is your responsibility to make it happen by reimplementing 'apply_event' method.
    As a rule of thumb there is a simple way to do it:

        def apply_event(event) -> None:
            # 1. Define routing from event type to event handler
            routing = {
                self.ItemAdded: self._when_item_added,
            }

            event_handler = routing.get(type(event))  # 2. Take the corresponding event handler from routing
            if event_handler is None:
                super().apply_event(event)  # Default implementation will raise error if there is no handler
            else:
                event_handler(event)  # Handle event

    Event handler should change the state of the aggregate:

        def _when_item_added(event) -> None:
            self._items.append(event.event_name)

    ACHTUNG - your event handlers MUST NOT contain any business logic, it only should change the state of the aggregate based on what is stated in the event.

    Next step - application module.
    """

    # TODO - register aggregate type automaticallys
    # TODO - event handlers take events as input and change the aggregate state
    # TODO - command handlers take commands as input, validate business logic and trigger events
    # TODO - aggregate state changes should occur only in event handlers, NOT command handlers, how to protect aggregate from occasional state change in command handler???

    INITIAL_VERSION = 0

    @dataclass(frozen=True)
    class Created(Created):
        """Use it as a base class for Created events"""
        pass

    @dataclass(frozen=True)
    class Event(DomainEvent):
        """Use it as a base class for consequtive events"""
        pass

    def __init__(self, event: Created):
        self._aggregate_id = event.metadata.aggregate_id
        self._aggregate_version = event.metadata.aggregate_version
        self._aggregate_created_on = datetime.fromisoformat(event.metadata.timestamp)
        self._aggregate_modified_on = datetime.fromisoformat(event.metadata.timestamp)

        self._pending_events: List[DomainEvent] = list()

    @property
    def aggregate_created_on(self) -> datetime:
        """The date and time when the aggregate was created."""
        return self._aggregate_created_on

    @property
    def aggregate_id(self) -> str:
        return self._aggregate_id

    @property
    def aggregate_modified_on(self) -> datetime:
        """The date and time when the aggregate was modified."""
        return self._aggregate_modified_on

    @property
    def aggregate_type(self) -> str:
        return self.get_aggregate_type()

    @property
    def aggregate_version(self) -> int:
        return self._aggregate_version

    @property
    def next_version(self) -> int:
        """Convenience property to get the next version number of aggregate"""
        return self.aggregate_version + 1

    @property
    def pending_events(self) -> Tuple[DomainEvent, ...]:
        return tuple(self._pending_events)

    @classmethod
    def _create(cls, event: 'Created') -> 'Aggregate':
        """Actually creates new aggregate. Should be used by factory method implemented on specific aggregate instance."""
        obj = cls(event=event)
        obj._pending_events.append(event)
        return obj

    def apply_event(self, event: DomainEvent) -> None:
        """
        Reimplement to provide change of state of the aggregate when event occur.
        Each event must have the associated event handler.
        """
        raise TypeError(f'Unknown Event {type(event)}')

    def clear_events(self) -> None:
        """Clears all pending events on the Aggregate"""
        self._pending_events = list()

    def collect_events(self) -> List[DomainEvent]:
        """Returns the list of pending events in the aggregate and clears pending events"""
        events = self._pending_events
        self.clear_events()
        return events

    @staticmethod
    @abstractmethod
    def create_id(*args, **kwargs) -> str:
        """
        Reimplement to create ID of the aggregate.
        Aggregate ID MUST BE UNIQUE accross the same aggregate type.
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def get_aggregate_type() -> str:
        """
        Aggregate type should be defined during modelling stage and MUST NOT BE CHANGED during the entire aggregate lifecycle.
        It is used by repository to retreive specific aggregate instances and by event store as prefix to event stream.
        """
        # TODO - something smells in here, is it possible to get rid of aggregate type???
        raise NotImplementedError

    def mutate(self, event: DomainEvent) -> None:
        """
        Performs validation of the event and changes the state of the aggregate by calling 'apply_event'.
        After application of the event modifies the version and modification date of the aggregate.

        Validation:
            - Event.aggregate_id should be equal to Aggregate.id. Raises AggregateIdError if not.
            - Event.aggregate_version should be EXACTLY ONE more than Aggregate.version. Raises AggregateVersionError if not.
        """
        # Event MUST belong to the aggregate
        if self.aggregate_id != event.metadata.aggregate_id:
            raise AggregateIDMismatch(f'ERROR - {event.__class__.__qualname__} aggregate_id does not match Aggregate ID attribute.')
        # Version of the new event MUST BE EXACTLY ONE MORE than the current aggegate version
        if (event.metadata.aggregate_version - self.aggregate_version) != 1:
            raise AggregateVersionError(f'ERROR - event aggregate version should be exactly one more than the aggregate version.')

        # Change the state of Aggregate
        self.apply_event(event)

        # Record new version and modification date
        self._aggregate_version = event.metadata.aggregate_version
        self._aggregate_modified_on = event.metadata.timestamp

    def trigger_event(self, event: Event) -> None:
        """
        Should be called whenever new event occur during aggregate lifecycle.
        Mutates aggregate state and adds event to the list of pending events
        """
        self.mutate(event)
        self._pending_events.append(event)
