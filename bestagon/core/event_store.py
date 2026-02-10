from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class NewStreamEvent:
    """New event to store in event store"""
    stream_position: int  # Position in aggreate sequence
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
    pass


class EventStoreSubscription(ABC):
    def __init__(self, name: str, parameters: SubscriptionParameters):
        self._name = name
        self._parameters = parameters
        self._running = True

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

    @property
    def parameters(self) -> SubscriptionParameters:
        return self._parameters

    @property
    def running(self) -> bool:
        return self._running

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
    async def append_events(self, stream_name: str, events: List[NewStreamEvent]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def create_subscription(self, subscription_name: str, subscription_parameters: 'SubscriptionParameters') -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_all(self, subscription_name: str, start_position: int) -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_events(self, subscription_name: str, events: List[str], start_position: int) -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_stream(self, subscription_name: str, regex: str, start_position: int) -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def get_stream(self, stream_name: str) -> Tuple[StreamEvent]:
        raise NotImplementedError

    @abstractmethod
    async def stream_exists(self, stream_name: str) -> bool:
        raise NotImplementedError
