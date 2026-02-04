from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Any, TYPE_CHECKING

from bestagon.core.mapper import mapper
from bestagon.core.message import ApplicationEvent

if TYPE_CHECKING:
    from bestagon.core.message import NewStreamEvent, StreamEvent


@dataclass(frozen=True)
class SubscriptionParameters:
    pass


class Subscription(ABC):
    def __init__(self, subscription_id: str, parameters: SubscriptionParameters):
        self._subscription_id = subscription_id
        self._parameters = parameters
        self._running = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.next_event()

    def __eq__(self, other):
        if isinstance(other, Subscription):
            return self.subscription_id == other.subscription_id
        return NotImplemented

    @property
    def parameters(self) -> SubscriptionParameters:
        return self._parameters

    @property
    def running(self) -> bool:
        return self._running

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def next_event(self) -> Any:
        raise NotImplementedError


class EventStoreSubscription(Subscription):
    @abstractmethod
    async def next_event(self) -> 'StreamEvent':
        raise NotImplementedError


class ApplicationSubscription(Subscription):
    def __init__(self, subscription_id: str, event_store_subscription: EventStoreSubscription):
        super().__init__(subscription_id=subscription_id, parameters=SubscriptionParameters())
        self._event_store_subscription = event_store_subscription

    async def next_event(self) -> ApplicationEvent:
        if not self.running:
            raise StopAsyncIteration

        stream_event = await self._event_store_subscription.next_event()
        domain_event = mapper.to_domain_event(stream_event=stream_event)
        application_event = ApplicationEvent(
            commit_position=stream_event.commit_position,
            domain_event=domain_event
        )
        return application_event

    async def stop(self) -> None:
        if self.running:
            self._running = False
            await self._event_store_subscription.stop()


class EventStore(ABC):
    def __init__(self):
        self._subscriptions: List['EventStoreSubscription'] = list()

    @property
    def subscriptions(self) -> Tuple['EventStoreSubscription', ...]:
        return tuple(self._subscriptions)

    @abstractmethod
    async def append_events(self, stream_name: str, events: List['NewStreamEvent']) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def create_subscription(self, subscription_id: str, subscription_parameters: 'SubscriptionParameters') -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_all(self, subscription_id: str, start_position: int) -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_events(self, subscription_id: str, events: List[str], start_position: int) -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def create_subscription_to_stream(self, subscription_id: str, regex: str, start_position: int) -> 'EventStoreSubscription':
        raise NotImplementedError

    @abstractmethod
    async def get_stream(self, stream_name: str) -> List['StreamEvent']:
        raise NotImplementedError

    @abstractmethod
    async def stream_exists(self, stream_name: str) -> bool:
        raise NotImplementedError
