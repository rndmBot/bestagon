import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import List, Tuple, Type, TYPE_CHECKING, Dict, Callable
from uuid import uuid4

from bestagon.core.exceptions import BestagonError

if TYPE_CHECKING:
    from bestagon.core.message import Command


class DomainException(BestagonError):
    """Should be raised in case of business rules violation"""
    pass


class AggregateIDMismatch(BestagonError):
    pass


class AggregateVersionError(BestagonError):
    pass


@dataclass(frozen=True)
class DomainEventContext:
    """
    Contains the contextual data that can be propagated from one event to another.

    :param event_id: ID of the event the context belongs to.
    :param correlation_id: a unique identifier attached to a request that remains consistent as the request passes through multiple services.
    :param trace_id: OpenTelemetry trace ID
    :param span_id: OpenTelemetry span ID
    """

    event_id: str | None = None
    correlation_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None

    @classmethod
    def from_command(cls, command: 'Command') -> 'DomainEventContext':
        obj = cls(
            event_id=command.metadata.causation_id,
            correlation_id=command.metadata.correlation_id,
            trace_id=command.metadata.trace_id,
            span_id=command.metadata.span_id
        )
        return obj

    @classmethod
    def from_domain_event(cls, event: 'DomainEvent') -> 'DomainEventContext':
        obj = cls(
            event_id=event.metadata.event_id,
            correlation_id=event.metadata.correlation_id,
            trace_id=event.metadata.trace_id,
            span_id=event.metadata.span_id
        )
        return obj


@dataclass(frozen=True)
class DomainEventMetadata:
    """
    Key domain event metadata. Contains all technical attributes of the event.

    :param event_id: Unique identifier for this specific event instance
    :param timestamp: ISO 8601 timestamp when the event was created
    :param aggregate_id: ID of the aggregate the event belongs to
    :param aggregate_version: version of the aggregate after creation of the event
    :param aggregate_type: type of the aggregate event belongs to

    :param correlation_id: a unique identifier attached to a request that remains consistent as the request passes through multiple services.
    :param causation_id: ID of the event that triggered this one
    :param trace_id: OpenTelemetry trace ID
    :param span_id: OpenTelemetry span ID
    """
    event_id: str
    timestamp: str
    aggregate_id: str
    aggregate_version: int
    aggregate_type: str
    # TODO - add event_type here???

    # Tracing identifiers
    correlation_id: str | None = None  # Groups related events from same user action
    causation_id: str | None = None  # ID of the event that triggered this one
    trace_id: str | None = None  # OpenTelemetry trace ID
    span_id: str| None = None  # OpenTelemetry span ID

    @staticmethod
    def create_event_id() -> str:
        return str(uuid4())

    @staticmethod
    def create_timestamp() -> str:
        return datetime.datetime.now(datetime.UTC).isoformat()

    @classmethod
    def from_aggregate(cls, aggregate: 'Aggregate', context: DomainEventContext | None = None) -> 'DomainEventMetadata':
        """
        Convenience factory method to create metadata from aggregate instance.

        :param aggregate: Aggregate class instance
        :param context: Additional contextual information from the previous event
        :return: DomainEventMetadata instance
        """
        obj = cls(
            event_id=cls.create_event_id(),
            timestamp=cls.create_timestamp(),
            aggregate_id=aggregate.aggregate_id,
            aggregate_version=aggregate.next_aggregate_version,
            aggregate_type=aggregate.aggregate_type,

            correlation_id=context.correlation_id if context is not None else None,
            causation_id=context.event_id if context is not None else None,
            trace_id=context.trace_id if context is not None else None,
            span_id=context.span_id if context is not None else None
        )
        return obj

    @classmethod
    def from_aggregate_class(cls, aggregate_cls: Type['Aggregate'], aggregate_id: str, context: DomainEventContext | None = None) -> 'DomainEventMetadata':
        """
        Convenience factory method to create metadata from aggregate class. Can be used before the aggregate creation,
        for example inside the aggregate's factory method.

        :param aggregate_cls: Aggregate class
        :param aggregate_id: before creation aggregate contain no aggregate_id, so it should be provided explicitly using this parameter.
        :param context: Additional contextual information from the previous event
        :return: DomainEventMetadata instance
        """


        obj = cls(
            event_id=cls.create_event_id(),
            timestamp=cls.create_timestamp(),
            aggregate_id=aggregate_id,
            aggregate_version=aggregate_cls.INITIAL_VERSION,
            aggregate_type=aggregate_cls.get_aggregate_type(),

            correlation_id=context.correlation_id if context is not None else None,
            causation_id=context.event_id if context is not None else None,
            trace_id=context.trace_id if context is not None else None,
            span_id=context.span_id if context is not None else None
        )
        return obj

    @classmethod
    def from_dict(cls, data: dict) -> 'DomainEventMetadata':
        obj = cls(**data)
        return obj

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class DomainEvent:
    """
    Domain event represents an important change in a business domain that is meaningfull to business experts and stakeholders.
    Domain event leads to the change in aggregate state and at the same time triggers a reaction in other parts of the system.

    :param metadata: non business related data that is important in the context of specificdomain event.
    """
    metadata: DomainEventMetadata

    def get_payload(self) -> dict:
        """
        Returns business related fields as a dictionary.
        """
        payload = asdict(self)
        payload.pop('metadata')
        return payload


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

    def __init__(self, event: Created):
        """
        The emthod takes `Created` event as input aprameter and sets the initial state of the aggregate.
        :param event:
        """
        self._aggregate_id = event.metadata.aggregate_id
        self._aggregate_version = event.metadata.aggregate_version
        self._aggregate_created_on = event.metadata.timestamp
        self._aggregate_modified_on = event.metadata.timestamp

        self._pending_events: List[DomainEvent] = list()

    @property
    def aggregate_created_on(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self._aggregate_created_on)

    @property
    def aggregate_id(self) -> str:
        return self._aggregate_id

    @property
    def aggregate_modified_on(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self._aggregate_modified_on)

    @property
    def aggregate_type(self) -> str:
        return self.get_aggregate_type()

    @property
    def aggregate_version(self) -> int:
        return self._aggregate_version

    @property
    def next_aggregate_version(self) -> int:
        """
        Convenience property to get the next version number of the aggregate
        """
        return self.aggregate_version + 1

    @property
    def pending_events(self) -> Tuple[DomainEvent, ...]:
        return tuple(self._pending_events)

    def apply_event(self, event: DomainEvent) -> None:
        """
        Reimplement to provide change of state of the aggregate when event occur.
        Each event must have the associated event handler.
        """
        event_routing = self.get_event_routing()
        event_handler = event_routing.get(type(event))
        if event_handler is not None:
            event_handler(event)
        else:
            raise NotImplementedError(
                f'Failed to apply event {type(event)} "{event.metadata.event_id}" to aggregate "{self.get_aggregate_type()}" - "{self.aggregate_id}": '
                f'no event handler provided for the event, please check `get_event_routing` method, it should contain '
                f'the routing to valid event handler, for example {{self.CustomerCreated: self._when_customer_created}}'
            )

    def clear_events(self) -> None:
        """Clears all pending events on the Aggregate"""
        self._pending_events = list()

    def collect_events(self) -> List[DomainEvent]:
        """Returns the list of pending events in the aggregate and clears pending events"""
        events = self._pending_events
        self.clear_events()
        return events

    @classmethod
    def create_aggregate(cls, event: 'Created') -> 'Aggregate':
        """Actually creates new aggregate. Should be used by factory method implemented on specific aggregate instance."""
        if event.metadata.aggregate_version != cls.INITIAL_VERSION:
            raise AggregateVersionError(
                f'Failed to create aggregate "{event.metadata.aggregate_type}" - '
                f'expected aggregate version {cls.INITIAL_VERSION}, got {event.metadata.aggregate_version}'
            )

        obj = cls(event=event)
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

    @staticmethod
    @abstractmethod
    def get_aggregate_type() -> str:
        """
        Aggregate type should be defined during modelling stage and MUST NOT BE CHANGED during the entire aggregate lifecycle.
        It is used by repository to retreive specific aggregate instances and by event store as prefix to event stream.
        """
        raise NotImplementedError

    @abstractmethod
    def get_event_routing(self) -> Dict[Type[DomainEvent], Callable]:
        """
        Event routing maps events to corresponding event handlers.
        Event handler is a method, defined in the aggregate class that takes event as input parameter
        and changes the state of the aggregate using values from the event.

        IMPORTANT - the change of the aggregate state should be made ONLY INSIDE THE EVENT HANDLER, never in any other
        part of the aggregate.

        Event routing example:
            {
                self.CustomerCreated: self._when_customer_created,
                self.CustomerBlocked: self._when_customer_blocked,
                ...
            }

        :return: mapping from event to corresponding event handler
        """
        raise NotImplementedError

    def mutate(self, event: DomainEvent) -> None:
        """
        The method takes a DomainEvent as an input and changes the aggregate state by applying the event.
        This method is used in two scenarios:
        1. Implicitly when calling `trigger_event` method when triggering newly created events.
        2. Explicitly when reconstructing the aggregate from events.
        """
        # Event MUST belong to the aggregate
        if self.aggregate_id != event.metadata.aggregate_id:
            raise AggregateIDMismatch(
                f'Failed t mutate aggregate "{self.aggregate_type}" {self.aggregate_id}: '
                f'aggregate_id of the event {event.metadata.event_id} does not match ID of the aggregate - {event.metadata.aggregate_id}. '
            )
        # Version of the new event MUST BE EXACTLY ONE MORE than the current aggegate version
        if (event.metadata.aggregate_version - self.aggregate_version) != 1:
            raise AggregateVersionError(
                f'Failed to mutate aggregate "{self.aggregate_type}" {self.aggregate_id}: '
                f'the version of the passed event must be exactly 1 more than the current version of the aggregate. '
                f'Current verion: {self.aggregate_version}, event version: {event.metadata.aggregate_version}. '
            )

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
