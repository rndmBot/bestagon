import asyncio
import logging
from dataclasses import dataclass
from typing import List, Union, Sequence, cast

import grpc
from kurrentdbclient import StreamState, NewEvent, DEFAULT_EXCLUDE_FILTER, AsyncKurrentDBClient, AsyncCatchupSubscription
from kurrentdbclient.common import DEFAULT_WINDOW_SIZE, DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER
from kurrentdbclient.exceptions import NotFoundError

from bestagon.core.event_store import StreamEvent, AsyncEventStore, NewStreamEvent
from bestagon.core.subscription import SubscriptionParameters, AsyncEventStoreSubscription
from bestagon.exceptions import IntegrityError


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


class AsyncKurrentDBSubscription(AsyncEventStoreSubscription):
    def __init__(self, subscription_id: str, parameters: KurrentDBSubscriptionParameters, kdb_subscription: AsyncCatchupSubscription):
        super().__init__(subscription_id=subscription_id, parameters=parameters)
        self._kdb_subscription = kdb_subscription

    async def next_event(self) -> StreamEvent:
        if not self.running:
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

    async def stop(self) -> None:
        self._running = True
        await self._kdb_subscription.stop()


class AsyncKurrentDBEventStore(AsyncEventStore):
    def __init__(self, client: AsyncKurrentDBClient):
        super().__init__()
        self.client = client

    async def append_events(self, stream_name: str, events: List[NewStreamEvent]) -> None:
        current_version = await self.client.get_current_version(stream_name=stream_name)
        first_event = events[0]
        if current_version == StreamState.NO_STREAM:
            if first_event.stream_position != 0:
                raise IntegrityError('Failed to append event to non existent stream, events position should start from 0')
        elif first_event.stream_position <= current_version:
            raise IntegrityError(f'Failed to append events in stream {stream_name} - event with version {first_event.stream_position} already exists in database')

        new_events = list()
        for event in events:
            new_event = NewEvent(
                type=event.event_type,
                data=event.payload,
                metadata=event.metadata
            )
            new_events.append(new_event)

        await self.client.append_events(stream_name=stream_name, current_version=current_version, events=new_events)

    async def close(self) -> None:
        if self. subscriptions:
            await asyncio.gather(*[sub.stop() for sub in self.subscriptions])
            logger.info('All subscriptions stopped')

        await self.client.close()
        logger.info('Event store closed')

    async def connect(self) -> None:
        await self.client.connect()
        logger.info('Event store connected')

    async def create_subscription(self, subscription_id: str, subscription_parameters: KurrentDBSubscriptionParameters) -> AsyncKurrentDBSubscription:
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

        subscription = AsyncKurrentDBSubscription(subscription_id=subscription_id, parameters=subscription_parameters, kdb_subscription=kdb_subscription)
        self._subscriptions.append(subscription)
        return subscription

    async def create_subscription_to_all(self, subscription_id: str, start_position: int) -> 'AsyncEventStoreSubscription':
        params = KurrentDBSubscriptionParameters(commit_position=start_position)
        sub = await self.create_subscription(subscription_id=subscription_id, subscription_parameters=params)
        return sub

    async def create_subscription_to_events(self, subscription_id: str, events: List[str], start_position: int) -> 'AsyncEventStoreSubscription':
        params = KurrentDBSubscriptionParameters(
            commit_position=start_position,
            filter_include=events,
            filter_by_stream_name=False
        )
        sub = await self.create_subscription(subscription_id=subscription_id, subscription_parameters=params)
        return sub

    async def create_subscription_to_stream(self, subscription_id: str, regex: str, start_position: int) -> AsyncEventStoreSubscription:
        params = KurrentDBSubscriptionParameters(
            commit_position=start_position,
            filter_include=[regex],
            filter_by_stream_name=True
        )
        return await self.create_subscription(subscription_id=subscription_id, subscription_parameters=params)

    async def get_stream(self, stream_name: str) -> List[StreamEvent]:
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
        return stream_events

    async def stream_exists(self, stream_name: str) -> bool:
        try:
            events = await self.client.get_stream(stream_name=stream_name, backwards=True, limit=1)
            return bool(events)
        except NotFoundError:
            return False
