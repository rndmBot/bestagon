from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple

from pydantic import BaseModel

from bestagon.core.exceptions import BestagonError


class SubscriptionError(BestagonError):
    pass


class NewStreamEvent(BaseModel):
    """New event to store in event store"""
    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class StreamEvent:
    """Event retreived from EventStore"""
    stream_name: str
    stream_position: int  # Position in aggreate sequence
    commit_position: int  # Position in event store sequence
    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class SubscriptionParameters:
    """
    This class should be reimplemented to provide subscription parameters for the specific technology,
    for example Kurrent database or AIOSQLite database
    """
    pass


class EventStoreSubscription(ABC):
    def __init__(self, name: str):
        self._name = name

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.next_event()

    def __eq__(self, other):
        if isinstance(other, EventStoreSubscription):
            return self.name == other.name
        return NotImplemented

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def is_running(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def next_event(self) -> StreamEvent:
        raise NotImplementedError


class EventStore(ABC):
    def __init__(self):
        self._subscriptions: List['EventStoreSubscription'] = list()

    @property
    def subscriptions(self) -> Tuple['EventStoreSubscription', ...]:
        return tuple(self._subscriptions)

    @abstractmethod
    async def append_events(self, stream_name: str, events: Tuple[NewStreamEvent]) -> None:
        """Reimplement to privde a logic to add new events in the event store."""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def create_subscription(self, subscription_name: str, subscription_parameters: 'SubscriptionParameters') -> 'EventStoreSubscription':
        """
        The base method that is responsible for the creation of subscription.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_all(self, subscription_name: str, start_position: int) -> 'EventStoreSubscription':
        """
        The event store should provide a functionality to create subscription to allevents in the database.
        IMPORTANT - Only events recorded after 'start_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_events(self, subscription_name: str, events: List[str], start_position: int) -> 'EventStoreSubscription':
        """
        The event store should provide functionality to subscribe only to specific event types.
        IMPORTANT - Only events recorded after 'start_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_stream(self, subscription_name: str, stream_name: str, start_position: int) -> 'EventStoreSubscription':
        """
        The event store should provide functionality to subscribe to a specific stream of events.
        IMPORTANT - Only events recorded after 'start_position' position will be obtained.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_stream(self, stream_name: str) -> Tuple[StreamEvent]:
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
    async def stream_exists(self, stream_name: str) -> bool:
        """
        Should return True if stream with provided name exists in the event store.
        """
        raise NotImplementedError
