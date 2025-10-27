from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from bestagon.core.subscription import AsyncEventStoreSubscription, SubscriptionParameters


@dataclass(frozen=True)
class NewStreamEvent:
    """New event to store in event store"""
    stream_position: int  # Position in aggreate sequence
    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class StreamEvent:
    """Event to be saved in and retreived from EventStore"""
    stream_name: str
    stream_position: int  # Position in aggreate sequence
    commit_position: int  # Position in application sequence
    event_type: str
    payload: bytes
    metadata: bytes

    def __eq__(self, other):
        if isinstance(other, StreamEvent):
            eq = all(
                [
                    self.stream_name == other.stream_name,
                    self.stream_position == other.stream_position
                ]
            )
            return eq
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, StreamEvent):
            return self.stream_position < other.stream_position
        return NotImplemented


class AsyncEventStore(ABC):
    def __init__(self):
        self._subscriptions: List['AsyncEventStoreSubscription'] = list()

    @property
    def subscriptions(self) -> Tuple['AsyncEventStoreSubscription', ...]:
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
    async def create_stream_subscription(self, subscription_id: str, regex: str, start_position: int) -> 'AsyncEventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def create_subscription(self, subscription_id: str, subscription_parameters: 'SubscriptionParameters') -> 'AsyncEventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def get_stream(self, stream_name: str) -> List[StreamEvent]:
        raise NotImplementedError

    @abstractmethod
    async def stream_exists(self, stream_name: str) -> bool:
        raise NotImplementedError
