from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from bestagon.core.exceptions import BestagonError


class SubscriptionError(BestagonError):
    pass


class ExpectedVersionError(BestagonError):
    """Should be raised if invalid version of stream have been passed when appending new events to event store."""
    def __init__(self, expected_version: int | None, current_version: int | None):
        super().__init__(f'Failed to append events to event store - the provided expected version "{expected_version}" '
                         f'does not match the current stream version "{current_version}".')


class OptimisticConcurrencyError(BestagonError):
    """Should be raised if there is attempt to append events to stream positin that is already recorded."""
    pass


class NewEventStoreEvent(BaseModel):
    """
    The class represent the new event to be appended to an event store.

    Fields:
        - event_type - a string with type of the event, for example 'CustomerCreated'.
        - payload - contains a business domain specific information about what have happened. Should be in bytes format.
        - metadata - contains non-business related data that is required for aggregate reconstruction and etc.
    """
    model_config = ConfigDict(strict=True)

    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class EventStoreEvent:
    """
    Event store events represent events that already saved in the event store. Every time you retrieve events from
    event store, they are returned in the form of EventStoreEvent.

    Fields:
        - commit_position - the position of an event in a global sequence of events across the whole event store
        - stream_position - the position of an event in a specific stream.
        - event_type - a string with a type of an event, for example 'AggregateCreated'
        - payload - contains a buiness specific information that answers what exactly have changed in your domain
        - metadata - contains non domain related information, for example timestamp when event created, id of aggregate that
        generated an event, etc.
    """
    stream_name: str
    stream_position: int  # Position in aggregate sequence
    commit_position: int  # Position in event store sequence
    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class SubscriptionParameters:
    """
    Each storage technology provides it's own set of parameters for subscription and this class should be
    reimplemented to provide subscription parameters for the specific technology, for example Kurrent event store or SQLite database.
    """
    pass


class EventStoreSubscription(ABC):
    """
    The base class for event store subscription.
    You usually do not instantiate subscription by yourself, it is responsibility of the event store.
    Each subscription contains unique identifier in the form of UUID4. In addition to identifier an optional name can be provided.

    Conceptually each subsctription is an AsyncIterator that can be awaited to receive new events.
    Examples of subscription usage:
    1. Directly calling next_event to receive the next event.
    2. Using async for syntax, for example:
        async for event in subscription:
            process_event(event)
    """
    def __init__(self, name: str | None = None):
        self._name = name
        self._id = str(uuid4())

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.next_event()

    def __eq__(self, other):
        if isinstance(other, EventStoreSubscription):
            return self.id == other.id
        return NotImplemented

    def __hash__(self):
        return hash(self.id)

    @property
    def id(self) -> str:
        """Each subscription is assigned a unique ID number"""
        return self._id

    @property
    def name(self) -> str | None:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self.set_name(name)

    @abstractmethod
    def is_running(self) -> bool:
        """
        The subscription should provide a way to check whether it is running or not.
        The running subscription can return events from the event store.
        Stopped subscription should raise an error on attempt to get next event.
        """
        raise NotImplementedError

    def set_name(self, name: str) -> None:
        self._name = name

    @abstractmethod
    async def start(self) -> None:
        """
        Reimplement to provide the logic to start the subscription.
        """
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        """
        Reimplement to provide the logic to stop the subscription.
        """
        raise NotImplementedError

    @abstractmethod
    async def next_event(self) -> EventStoreEvent:
        """
        When awaited the method should return the next event to be processed. If there are no new events in the
        event store then then method should return control to the event loop until the new event appears.
        """
        raise NotImplementedError


class EventStore(ABC):
    """
    Abstract interface for the event store.
    To be able to serve as a backbone of event-sourced system, the event store should satisfy multiple requirements:
        - A resilient source of truth - the events, stored in the event store are the source of truth, the event store should
          be able to keep these events as long as system lives.

        - Append-only - event store must ensure that new events can be only appended to the end of
          event stream and no event should be written to the begginning or in the middle of the stream.

        - Immutable - there should be no possibility to modify already recorded event.

        - Atomic writes across multiple events - if multiple events are appended to the event store the all of them should
          be persisted in one atomic transaction or none of them in case of failure. There should be no partial writes.

        - Optimistic concurrency per stream - event store should implement the optimistic concurrency mechanism, if two
          or more clients modify a single stream at the same time, then only one should succeed.

        - Two different read patterns - the event store should provide the posibility to read the entire history of events across all streams ('all' stream),
          and the possibility to read all events for the specific event stream.

        - Subscriptions - event store should provide a mechnism to subscribe to the necessary events and receive new ones when they arrive.
    """

    @abstractmethod
    async def append_events(
            self,
            stream_name: str,
            events: Tuple[NewEventStoreEvent, ...],
            expected_version: int | None
    ) -> None:
        """
        The method allows to add new events to the stream. It should satisfy three criteria:
            - Append-only - all events should be appended to the end of the stream.
            - Atomic writes across multiple events - all events should be written at once or none should be written in case of error.
            - Optimistic concurrency per stream - there should be a mechanism for optimistic concurrency

        :param stream_name: a name of the stream to append events
        :param events: a tuple of NewStreamEvents to append to the stream
        :param expected_version: stream position of the last appended event, required for optimistic concurrency control.

        :raises ExpectedVersionError: if expected version does not match current stream version
        :raises OptimisticConcurrencyError: if there is attempt to append an event to stream position that was already recorded.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription(self, subscription_name: str, subscription_parameters: SubscriptionParameters) -> EventStoreSubscription:
        """
        The method creates a subscriptin based on the provided parameters.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_all(self, subscription_name: str, last_commit_position: int | None) -> EventStoreSubscription:
        """
        The event store should provide a functionality to create subscription to all events in the database.
        IMPORTANT - Only events recorded after 'last_commit_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_events(self, subscription_name: str, event_types: List[str], last_commit_position: int | None) -> EventStoreSubscription:
        """
        The event store should provide functionality to subscribe only to specific event types.
        IMPORTANT - Only events recorded after 'last_commit_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_stream(self, subscription_name: str, stream_name: str, last_commit_position: int | None) -> EventStoreSubscription:
        """
        The event store should provide functionality to subscribe to a specific stream of events.
        IMPORTANT - Only events recorded after 'last_commit_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_stream(self, stream_name: str) -> Tuple[EventStoreEvent, ...]:
        """
        Reimplement to return all event from the specified stream.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_stream_version(self, stream_name: str) -> int | None:
        """
        Should return the current version of the stream or None if stream not exists.
        """
        raise NotImplementedError

    @abstractmethod
    def get_subscriptions(self) -> Tuple[EventStoreSubscription, ...]:
        """
        The method should return a tuple of subscriptions, that have been created by the event store.
        """
        raise NotImplementedError

    @abstractmethod
    async def initialize(self) -> None:
        """
        Event store can require additional actions after instantiation, for example to create a table for
        events if you use SQL database as an underlying technology. In such cases this method can be reimplemented
        to provide necvessary setup.
        """
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self) -> None:
        """
        This method can be reimplemented in case when you finishing to work with event store to release resources and
        finish the work gracefuly.
        """
        raise NotImplementedError

    @abstractmethod
    async def stream_exists(self, stream_name: str) -> bool:
        """
        Should return True if stream with provided name exists in the event store.
        """
        raise NotImplementedError
