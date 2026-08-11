from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from bestagon.core.exceptions import BestagonError


class SubscriptionError(BestagonError):
    pass


class NewStreamEvent(BaseModel):
    """
    New event to store in an event store.
    This class have to be used when you save new events in the event store.
    """
    model_config = ConfigDict(strict=True)

    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class StreamEvent:
    """
    Stream events represent events that already saved in the event store and they are returned every time
    you retrieve events from it.

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
    Each storage technology provides it's own set of parameters for subscription, therefore, this class should be
    reimplemented to provide subscription parameters for the specific technology,
    for example Kurrent database or AIOSQLite database
    """
    pass


class EventStoreSubscription(ABC):
    """
    The base class for event store subscription.
    The subscription is identified by it's ID, which is a UUID4, but you can also provide an optional name during initialization.

    One of the most important methods for subscription - next_event. When awaited, it should return the next event to process.

    The subscription can be used in several ways:
    1. Directly calling next_event t receive the next event.
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
        return self._id

    @property
    def name(self) -> str | None:
        return self._name

    @abstractmethod
    async def is_running(self) -> bool:
        """
        The subscription should provide a way to check whether it is running or not.
        The running subscription can return events from the event store.
        """
        raise NotImplementedError

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
    async def next_event(self) -> StreamEvent:
        """
        When awaited the method should return the next event to be processed. If there are no new events in the
        event store then then method should return control to the event loop until the new event appears.
        """
        raise NotImplementedError


class EventStore(ABC):
    @abstractmethod
    async def append_events(self, stream_name: str, events: Tuple[NewStreamEvent, ...]) -> None:
        """Reimplement to provide a logic to add new events in the event store."""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def create_subscription(self, subscription_name: str, subscription_parameters: SubscriptionParameters) -> EventStoreSubscription:
        """
        The base method that is responsible for the creation of subscription.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_all(self, subscription_name: str, start_position: int) -> EventStoreSubscription:
        """
        The event store should provide a functionality to create subscription to all events in the database.
        IMPORTANT - Only events recorded after 'start_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_events(self, subscription_name: str, event_types: List[str], start_position: int) -> EventStoreSubscription:
        """
        The event store should provide functionality to subscribe only to specific event types.
        IMPORTANT - Only events recorded after 'start_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_stream(self, subscription_name: str, stream_name: str, start_position: int) -> EventStoreSubscription:
        """
        The event store should provide functionality to subscribe to a specific stream of events.
        IMPORTANT - Only events recorded after 'start_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_stream(self, stream_name: str) -> Tuple[StreamEvent, ...]:
        """
        Reimplement to return all event from the specified stream.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_stream_version(self, stream_name: str) -> int:
        """
        Should return the current version of the stream or -1 if stream not exists.
        """
        raise NotImplementedError

    @abstractmethod
    def get_subscriptions(self) -> Tuple[EventStoreSubscription, ...]:
        raise NotImplementedError

    @abstractmethod
    async def stream_exists(self, stream_name: str) -> bool:
        """
        Should return True if stream with provided name exists in the event store.
        """
        raise NotImplementedError
