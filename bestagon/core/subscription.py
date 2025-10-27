from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING

from bestagon.core.mapper import Mapper
from bestagon.core.message import ApplicationEvent

if TYPE_CHECKING:
    from bestagon.core.event_store import StreamEvent


@dataclass(frozen=True)
class SubscriptionParameters:
    pass


class AsyncSubscription(ABC):
    def __init__(self, subscription_id: str, parameters: SubscriptionParameters):
        self._subscription_id = subscription_id
        self._parameters = parameters
        self._running = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.next_event()

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
    async def next_event(self) -> any:
        raise NotImplementedError


class AsyncEventStoreSubscription(AsyncSubscription):
    @abstractmethod
    async def next_event(self) -> 'StreamEvent':
        raise NotImplementedError


class AsyncApplicationSubscription(AsyncSubscription):
    def __init__(
            self,
            subscription_id: str,
            application_name: str,
            mapper: Mapper,
            event_store_subscription: AsyncEventStoreSubscription
    ):
        super().__init__(subscription_id=subscription_id, parameters=SubscriptionParameters())
        self._application_name = application_name
        self._mapper = mapper
        self._event_store_subscription = event_store_subscription

    @property
    def application_name(self) -> str:
        return self._application_name

    async def next_event(self) -> ApplicationEvent:
        if not self.running:
            raise StopAsyncIteration

        stream_event = await self._event_store_subscription.next_event()
        domain_event = self._mapper.to_domain_event(stream_event=stream_event)
        application_event = ApplicationEvent(
            commit_position=stream_event.commit_position,
            domain_event=domain_event
        )
        return application_event

    async def stop(self) -> None:
        self._running = False
        await self._event_store_subscription.stop()
