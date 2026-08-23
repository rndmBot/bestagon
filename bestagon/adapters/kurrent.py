import asyncio
import logging
from dataclasses import dataclass
from typing import List, Union, Sequence, cast, Tuple

import grpc
from kurrentdbclient import StreamState, NewEvent, DEFAULT_EXCLUDE_FILTER, AsyncKurrentDBClient, AsyncCatchupSubscription
from kurrentdbclient.common import DEFAULT_WINDOW_SIZE, DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER
from kurrentdbclient.exceptions import NotFoundError, WrongCurrentVersionError

from bestagon.core.event_store import EventStore, SubscriptionParameters, EventStoreSubscription, StreamEvent, \
    NewStreamEvent, ExpectedVersionError, SubscriptionError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KurrentDBSubscriptionParameters(SubscriptionParameters):
    commit_position: Union[int, None] = None
    from_end: bool = False
    resolve_links: bool = False
    filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER
    filter_include: Sequence[str] = ()
    filter_by_stream_name: bool = False
    include_checkpoints: bool = False
    window_size: int = DEFAULT_WINDOW_SIZE
    checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER
    include_caught_up: bool = False
    include_fell_behind: bool = False
    timeout: Union[float, None] = None
    credentials: Union[grpc.CallCredentials, None] = None


class KurrentDBSubscription(EventStoreSubscription):
    def __init__(self, name: str, kdb_subscription: AsyncCatchupSubscription):
        super().__init__(name=name)
        self._kdb_subscription = kdb_subscription
        self._running = False

    def is_running(self) -> bool:
        return self._running

    async def next_event(self) -> StreamEvent:
        if not self._running:
            raise StopAsyncIteration

        event = await anext(self._kdb_subscription)
        stream_event = StreamEvent(
            stream_name=event.stream_name,
            stream_position=event.stream_position,
            commit_position=event.commit_position,
            event_type=event.type,
            payload=event.data,
            metadata=event.metadata
        )
        return stream_event

    async def start(self) -> None:
        if self.is_running():
            raise SubscriptionError(f'Subscription {self.name} already started')
        self._running = True

    async def stop(self) -> None:
        if not self.is_running():
            raise SubscriptionError(f'Subscription {self.name} is already stopped')

        self._running = False
        await self._kdb_subscription.stop()


class KurrentDBEventStore(EventStore):
    def __init__(self, client: AsyncKurrentDBClient):
        super().__init__()
        self._subscriptions: List[KurrentDBSubscription] = list()
        self.client = client

    async def append_events(self, stream_name: str, events: Tuple[NewStreamEvent, ...], expected_version: int | None = None) -> None:
        if not events:
            return
        if not all(isinstance(e, NewStreamEvent) for e in events):
            raise TypeError(
                f'Failed to append events into {self.__class__.__qualname__}, '
                f'all events must be instances of NewStreamEvent class, '
                f'one or more events are of invalid type, please check types of the passed events.'
            )

        new_events = list()
        for event in events:
            new_event = NewEvent(
                type=event.event_type,
                data=event.payload,
                metadata=event.metadata
            )
            new_events.append(new_event)

        stream_version = await self.get_stream_version(stream_name=stream_name)
        current_version = StreamState.NO_STREAM if expected_version is None else expected_version
        try:
            logger.debug(f'Appending {len(events)} events into "{stream_name}" stream of {self.__class__.__qualname__}')
            await self.client.append_events(stream_name=stream_name, current_version=current_version, events=new_events)
            logger.debug(f'New events appended to "{stream_name}" stream of {self.__class__.__qualname__}')
        except WrongCurrentVersionError:
            raise ExpectedVersionError(expected_version=expected_version, current_version=stream_version)

    async def create_subscription(
            self, subscription_name: str,
            subscription_parameters: KurrentDBSubscriptionParameters
    ) -> KurrentDBSubscription:
        logger.debug(f'Creating subscription to {self.__class__.__qualname__} with parameters {subscription_parameters}')
        kdb_subscription = await self.client.subscribe_to_all(
            commit_position=subscription_parameters.commit_position,
            from_end=subscription_parameters.from_end,
            resolve_links=subscription_parameters.resolve_links,
            filter_exclude=subscription_parameters.filter_exclude,
            filter_include=subscription_parameters.filter_include,
            filter_by_stream_name=subscription_parameters.filter_by_stream_name,
            include_checkpoints=subscription_parameters.include_checkpoints,
            window_size=subscription_parameters.window_size,
            checkpoint_interval_multiplier=subscription_parameters.checkpoint_interval_multiplier,
            include_caught_up=subscription_parameters.include_caught_up,
            include_fell_behind=subscription_parameters.include_fell_behind,
            timeout=subscription_parameters.timeout,
            credentials=subscription_parameters.credentials
        )
        kdb_subscription = cast(AsyncCatchupSubscription, kdb_subscription)

        subscription = KurrentDBSubscription(name=subscription_name, kdb_subscription=kdb_subscription)
        await subscription.start()
        self._subscriptions.append(subscription)
        return subscription

    async def create_subscription_to_all(
            self,
            subscription_name: str,
            last_commit_position: int
    ) -> 'EventStoreSubscription':
        params = KurrentDBSubscriptionParameters(commit_position=last_commit_position)
        sub = await self.create_subscription(subscription_name=subscription_name, subscription_parameters=params)
        return sub

    async def create_subscription_to_events(
            self,
            subscription_name: str,
            event_types: List[str],
            last_commit_position: int
    ) -> 'EventStoreSubscription':
        params = KurrentDBSubscriptionParameters(
            commit_position=last_commit_position,
            filter_include=event_types,
            filter_by_stream_name=False
        )
        sub = await self.create_subscription(subscription_name=subscription_name, subscription_parameters=params)
        return sub

    async def create_subscription_to_stream(
            self,
            subscription_name: str,
            stream_name: str,
            last_commit_position: int
    ) -> EventStoreSubscription:
        params = KurrentDBSubscriptionParameters(
            commit_position=last_commit_position,
            filter_include=[stream_name],
            filter_by_stream_name=True
        )
        return await self.create_subscription(subscription_name=subscription_name, subscription_parameters=params)

    async def get_stream_version(self, stream_name: str) -> int | None:
        version = await self.client.get_current_version(stream_name=stream_name)
        if version == StreamState.NO_STREAM:
            return None
        return version

    async def get_stream(self, stream_name: str) -> Tuple[StreamEvent]:
        events = await self.client.get_stream(stream_name=stream_name)
        stream_events = list()
        for event in events:
            stream_event = StreamEvent(
                stream_name=stream_name,
                stream_position=event.stream_position,
                commit_position=event.commit_position,
                event_type=event.type,
                payload=event.data,
                metadata=event.metadata
            )
            stream_events.append(stream_event)
        return tuple(stream_events)

    def get_subscriptions(self) -> Tuple[KurrentDBSubscription, ...]:
        return tuple(self._subscriptions)

    async def initialize(self) -> None:
        logger.info(f'Initializing {self.__class__.__qualname__} event store')
        logger.info(f'{self.__class__.__qualname__} event store initialized')

    async def shutdown(self) -> None:
        logger.info(f'Shuting down {self.__class__.__qualname__}')
        await asyncio.gather(*[sub.stop() for sub in self._subscriptions if sub.is_running()])
        logger.info(f'{self.__class__.__qualname__} shut down')

    async def stream_exists(self, stream_name: str) -> bool:
        try:
            events = await self.client.get_stream(stream_name=stream_name, backwards=True, limit=1)
            return bool(events)
        except NotFoundError:
            return False
