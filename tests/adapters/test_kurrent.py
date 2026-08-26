from dataclasses import FrozenInstanceError
from typing import Sequence
from unittest.mock import create_autospec
from uuid import uuid4

import pytest
from kurrentdbclient import (
    DEFAULT_EXCLUDE_FILTER,
    AsyncCatchupSubscription,
    AsyncKurrentDBClient,
    NewEvent,
    RecordedEvent,
    StreamState,
)
from kurrentdbclient.common import DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER, DEFAULT_WINDOW_SIZE
from kurrentdbclient.exceptions import NotFoundError, WrongCurrentVersionError

from bestagon.adapters.kurrent import (
    KurrentDBEventStore,
    KurrentDBSubscription,
    KurrentDBSubscriptionParameters,
)
from bestagon.core.event_store import ExpectedVersionError, NewEventStoreEvent, EventStoreEvent, SubscriptionError


def new_event(event_type: str = 'TestEvent', payload: bytes = b'payload', metadata: bytes = b'metadata') -> NewEventStoreEvent:
    return NewEventStoreEvent(event_type=event_type, payload=payload, metadata=metadata)


def recorded_event(
    event_type: str = 'TestEvent',
    stream_name: str = 'stream-a',
    stream_position: int = 0,
    commit_position: int = 1,
    payload: bytes = b'payload',
    metadata: bytes = b'metadata',
) -> RecordedEvent:
    return RecordedEvent(
        type=event_type,
        data=payload,
        metadata=metadata,
        content_type='application/json',
        id=uuid4(),
        stream_name=stream_name,
        stream_position=stream_position,
        commit_position=commit_position,
        prepare_position=commit_position,
    )


class FakeCatchupSubscription:
    """Stand-in for AsyncCatchupSubscription: yields a fixed set of events, then stops."""

    def __init__(self, events: Sequence[RecordedEvent] = ()):
        self._events = list(events)
        self.stop_call_count = 0

    def __aiter__(self) -> 'FakeCatchupSubscription':
        return self

    async def __anext__(self) -> RecordedEvent:
        if not self._events:
            raise StopAsyncIteration
        return self._events.pop(0)

    async def stop(self) -> None:
        self.stop_call_count += 1


@pytest.fixture
def kdb_subscription() -> FakeCatchupSubscription:
    return FakeCatchupSubscription()


@pytest.fixture
def client() -> AsyncKurrentDBClient:
    return create_autospec(AsyncKurrentDBClient, spec_set=True, instance=True)


@pytest.fixture
def event_store(client: AsyncKurrentDBClient) -> KurrentDBEventStore:
    client.get_current_version.return_value = StreamState.NO_STREAM
    client.get_stream.return_value = ()
    client.subscribe_to_all.return_value = FakeCatchupSubscription()
    return KurrentDBEventStore(client=client)


class TestKurrentDBSubscriptionParameters:
    def test_defaults_match_the_kurrentdb_client_defaults(self):
        parameters = KurrentDBSubscriptionParameters()

        assert parameters.commit_position is None
        assert parameters.from_end is False
        assert parameters.resolve_links is False
        assert parameters.filter_exclude == DEFAULT_EXCLUDE_FILTER
        assert parameters.filter_include == ()
        assert parameters.filter_by_stream_name is False
        assert parameters.include_checkpoints is False
        assert parameters.window_size == DEFAULT_WINDOW_SIZE
        assert parameters.checkpoint_interval_multiplier == DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER
        assert parameters.include_caught_up is False
        assert parameters.include_fell_behind is False
        assert parameters.timeout is None
        assert parameters.credentials is None

    def test_parameters_are_immutable(self):
        parameters = KurrentDBSubscriptionParameters()

        with pytest.raises(FrozenInstanceError):
            parameters.commit_position = 10


class TestKurrentDBSubscription:
    def test_is_running_false_before_start(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)

        assert subscription.is_running() is False

    def test_subscription_keeps_its_name_and_gets_a_unique_id(self, kdb_subscription):
        first = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        second = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)

        assert first.name == 'sub'
        assert first.id != second.id
        assert first != second
        assert len({first, second, first}) == 2

    @pytest.mark.asyncio
    async def test_start_marks_subscription_as_running(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)

        await subscription.start()

        assert subscription.is_running() is True

    @pytest.mark.asyncio
    async def test_start_raises_when_already_running(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()

        with pytest.raises(SubscriptionError):
            await subscription.start()

    @pytest.mark.asyncio
    async def test_stop_marks_subscription_as_not_running_and_stops_the_client_subscription(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()

        await subscription.stop()

        assert subscription.is_running() is False
        assert kdb_subscription.stop_call_count == 1

    @pytest.mark.asyncio
    async def test_stop_raises_when_never_started(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)

        with pytest.raises(SubscriptionError):
            await subscription.stop()

        assert kdb_subscription.stop_call_count == 0

    @pytest.mark.asyncio
    async def test_stop_raises_when_already_stopped(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()
        await subscription.stop()

        with pytest.raises(SubscriptionError):
            await subscription.stop()

        assert kdb_subscription.stop_call_count == 1

    @pytest.mark.asyncio
    async def test_restart_after_stop_is_allowed(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()
        await subscription.stop()

        await subscription.start()

        assert subscription.is_running() is True

    @pytest.mark.asyncio
    async def test_next_event_raises_stop_async_iteration_when_not_running(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)

        with pytest.raises(StopAsyncIteration):
            await subscription.next_event()

    @pytest.mark.asyncio
    async def test_next_event_raises_stop_async_iteration_after_stop(self):
        kdb_subscription = FakeCatchupSubscription([recorded_event()])
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()
        await subscription.stop()

        with pytest.raises(StopAsyncIteration):
            await subscription.next_event()

    @pytest.mark.asyncio
    async def test_next_event_maps_recorded_event_onto_stream_event(self):
        kdb_subscription = FakeCatchupSubscription([
            recorded_event(
                event_type='SomethingHappened',
                stream_name='stream-a',
                stream_position=3,
                commit_position=42,
                payload=b'the-payload',
                metadata=b'the-metadata',
            )
        ])
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()

        event = await subscription.next_event()

        assert event == EventStoreEvent(
            stream_name='stream-a',
            stream_position=3,
            commit_position=42,
            event_type='SomethingHappened',
            payload=b'the-payload',
            metadata=b'the-metadata',
        )

    @pytest.mark.asyncio
    async def test_next_event_returns_events_in_order(self):
        kdb_subscription = FakeCatchupSubscription([
            recorded_event(event_type='E0', stream_position=0, commit_position=1),
            recorded_event(event_type='E1', stream_position=1, commit_position=2),
        ])
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()

        first = await subscription.next_event()
        second = await subscription.next_event()

        assert [first.event_type, second.event_type] == ['E0', 'E1']

    @pytest.mark.asyncio
    async def test_next_event_propagates_exhaustion_of_the_client_subscription(self, kdb_subscription):
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()

        with pytest.raises(StopAsyncIteration):
            await subscription.next_event()

    @pytest.mark.asyncio
    async def test_async_iteration_yields_events(self):
        kdb_subscription = FakeCatchupSubscription([
            recorded_event(event_type='Iter1'),
            recorded_event(event_type='Iter2'),
        ])
        subscription = KurrentDBSubscription(name='sub', kdb_subscription=kdb_subscription)
        await subscription.start()

        received = [event.event_type async for event in subscription]

        assert received == ['Iter1', 'Iter2']


class TestKurrentDBEventStoreAppendEvents:
    @pytest.mark.asyncio
    async def test_empty_tuple_is_noop(self, event_store, client):
        await event_store.append_events('stream-a', (), expected_version=None)

        client.append_events.assert_not_called()
        client.get_current_version.assert_not_called()

    @pytest.mark.asyncio
    async def test_rejects_invalid_event_type(self, event_store, client):
        with pytest.raises(TypeError):
            await event_store.append_events('stream-a', ('not-a-new-stream-event',), expected_version=None)

        client.append_events.assert_not_called()

    @pytest.mark.asyncio
    async def test_rejects_mixed_valid_and_invalid_events(self, event_store, client):
        with pytest.raises(TypeError):
            await event_store.append_events('stream-a', (new_event(), object()), expected_version=None)

        client.append_events.assert_not_called()

    @pytest.mark.asyncio
    async def test_maps_new_stream_events_onto_client_events(self, event_store, client):
        events = (
            new_event(event_type='E0', payload=b'p0', metadata=b'm0'),
            new_event(event_type='E1', payload=b'p1', metadata=b'm1'),
        )

        await event_store.append_events('stream-a', events, expected_version=None)

        _, kwargs = client.append_events.call_args
        assert kwargs['stream_name'] == 'stream-a'
        assert [(e.type, e.data, e.metadata) for e in kwargs['events']] == [
            ('E0', b'p0', b'm0'),
            ('E1', b'p1', b'm1'),
        ]
        assert all(isinstance(e, NewEvent) for e in kwargs['events'])

    @pytest.mark.asyncio
    async def test_expected_version_none_is_translated_to_no_stream(self, event_store, client):
        await event_store.append_events('stream-a', (new_event(),), expected_version=None)

        assert client.append_events.call_args.kwargs['current_version'] is StreamState.NO_STREAM

    @pytest.mark.asyncio
    async def test_expected_version_is_passed_as_current_version(self, event_store, client):
        client.get_current_version.return_value = 4

        await event_store.append_events('stream-a', (new_event(),), expected_version=4)

        assert client.append_events.call_args.kwargs['current_version'] == 4

    @pytest.mark.asyncio
    async def test_expected_version_zero_is_not_confused_with_none(self, event_store, client):
        client.get_current_version.return_value = 0

        await event_store.append_events('stream-a', (new_event(),), expected_version=0)

        assert client.append_events.call_args.kwargs['current_version'] == 0

    @pytest.mark.asyncio
    async def test_expected_version_defaults_to_no_stream(self, event_store, client):
        await event_store.append_events('stream-a', (new_event(),))

        assert client.append_events.call_args.kwargs['current_version'] is StreamState.NO_STREAM

    @pytest.mark.asyncio
    async def test_wrong_current_version_is_translated_to_expected_version_error(self, event_store, client):
        client.get_current_version.return_value = 7
        client.append_events.side_effect = WrongCurrentVersionError()

        with pytest.raises(ExpectedVersionError) as error:
            await event_store.append_events('stream-a', (new_event(),), expected_version=2)

        assert '"2"' in str(error.value)
        assert '"7"' in str(error.value)

    @pytest.mark.asyncio
    async def test_wrong_current_version_reports_none_for_missing_stream(self, event_store, client):
        client.get_current_version.return_value = StreamState.NO_STREAM
        client.append_events.side_effect = WrongCurrentVersionError()

        with pytest.raises(ExpectedVersionError) as error:
            await event_store.append_events('stream-a', (new_event(),), expected_version=3)

        assert '"3"' in str(error.value)
        assert 'version "None"' in str(error.value)

    @pytest.mark.asyncio
    async def test_other_client_errors_are_propagated(self, event_store, client):
        client.append_events.side_effect = NotFoundError()

        with pytest.raises(NotFoundError):
            await event_store.append_events('stream-a', (new_event(),), expected_version=None)


class TestKurrentDBEventStoreReads:
    @pytest.mark.asyncio
    async def test_get_stream_version_returns_none_for_missing_stream(self, event_store, client):
        client.get_current_version.return_value = StreamState.NO_STREAM

        assert await event_store.get_stream_version('missing') is None
        client.get_current_version.assert_called_once_with(stream_name='missing')

    @pytest.mark.asyncio
    async def test_get_stream_version_returns_current_version(self, event_store, client):
        client.get_current_version.return_value = 5

        assert await event_store.get_stream_version('stream-a') == 5

    @pytest.mark.asyncio
    async def test_get_stream_version_returns_zero_for_single_event_stream(self, event_store, client):
        client.get_current_version.return_value = 0

        assert await event_store.get_stream_version('stream-a') == 0

    @pytest.mark.asyncio
    async def test_get_stream_returns_empty_tuple_for_empty_stream(self, event_store, client):
        client.get_stream.return_value = ()

        assert await event_store.get_stream('stream-a') == ()

    @pytest.mark.asyncio
    async def test_get_stream_maps_recorded_events_onto_stream_events(self, event_store, client):
        client.get_stream.return_value = (
            recorded_event(event_type='E0', stream_position=0, commit_position=1, payload=b'p0', metadata=b'm0'),
            recorded_event(event_type='E1', stream_position=1, commit_position=2, payload=b'p1', metadata=b'm1'),
        )

        stream = await event_store.get_stream('stream-a')

        assert stream == (
            EventStoreEvent(
                stream_name='stream-a',
                stream_position=0,
                commit_position=1,
                event_type='E0',
                payload=b'p0',
                metadata=b'm0',
            ),
            EventStoreEvent(
                stream_name='stream-a',
                stream_position=1,
                commit_position=2,
                event_type='E1',
                payload=b'p1',
                metadata=b'm1',
            ),
        )
        client.get_stream.assert_called_once_with(stream_name='stream-a')

    @pytest.mark.asyncio
    async def test_get_stream_uses_the_requested_stream_name(self, event_store, client):
        client.get_stream.return_value = (recorded_event(stream_name='ignored-by-adapter'),)

        stream = await event_store.get_stream('stream-a')

        assert stream[0].stream_name == 'stream-a'

    @pytest.mark.asyncio
    async def test_stream_exists_returns_true_when_stream_has_events(self, event_store, client):
        client.get_stream.return_value = (recorded_event(),)

        assert await event_store.stream_exists('stream-a') is True
        client.get_stream.assert_called_once_with(stream_name='stream-a', backwards=True, limit=1)

    @pytest.mark.asyncio
    async def test_stream_exists_returns_false_when_stream_is_empty(self, event_store, client):
        client.get_stream.return_value = ()

        assert await event_store.stream_exists('stream-a') is False

    @pytest.mark.asyncio
    async def test_stream_exists_returns_false_when_stream_not_found(self, event_store, client):
        client.get_stream.side_effect = NotFoundError()

        assert await event_store.stream_exists('missing') is False


class TestKurrentDBEventStoreSubscriptions:
    @pytest.mark.asyncio
    async def test_create_subscription_starts_and_tracks_subscription(self, event_store):
        subscription = await event_store.create_subscription(
            subscription_name='sub',
            subscription_parameters=KurrentDBSubscriptionParameters()
        )

        assert isinstance(subscription, KurrentDBSubscription)
        assert subscription.name == 'sub'
        assert subscription.is_running() is True
        assert event_store.get_subscriptions() == (subscription,)

    @pytest.mark.asyncio
    async def test_create_subscription_forwards_every_parameter_to_the_client(self, event_store, client):
        parameters = KurrentDBSubscriptionParameters(
            commit_position=17,
            from_end=True,
            resolve_links=True,
            filter_exclude=('excluded',),
            filter_include=('included',),
            filter_by_stream_name=True,
            include_checkpoints=True,
            window_size=7,
            checkpoint_interval_multiplier=3,
            include_caught_up=True,
            include_fell_behind=True,
            timeout=1.5,
        )

        await event_store.create_subscription(subscription_name='sub', subscription_parameters=parameters)

        client.subscribe_to_all.assert_called_once_with(
            commit_position=17,
            from_end=True,
            resolve_links=True,
            filter_exclude=('excluded',),
            filter_include=('included',),
            filter_by_stream_name=True,
            include_checkpoints=True,
            window_size=7,
            checkpoint_interval_multiplier=3,
            include_caught_up=True,
            include_fell_behind=True,
            timeout=1.5,
            credentials=None,
        )

    @pytest.mark.asyncio
    async def test_create_subscription_wraps_the_client_subscription(self, event_store, client):
        kdb_subscription = FakeCatchupSubscription([recorded_event(event_type='Wrapped')])
        client.subscribe_to_all.return_value = kdb_subscription

        subscription = await event_store.create_subscription(
            subscription_name='sub',
            subscription_parameters=KurrentDBSubscriptionParameters()
        )

        assert (await subscription.next_event()).event_type == 'Wrapped'

    @pytest.mark.asyncio
    async def test_create_subscription_to_all_subscribes_without_filters(self, event_store, client):
        await event_store.create_subscription_to_all(subscription_name='sub-all', last_commit_position=9)

        kwargs = client.subscribe_to_all.call_args.kwargs
        assert kwargs['commit_position'] == 9
        assert kwargs['filter_include'] == ()
        assert kwargs['filter_exclude'] == DEFAULT_EXCLUDE_FILTER
        assert kwargs['filter_by_stream_name'] is False

    @pytest.mark.asyncio
    async def test_create_subscription_to_events_filters_by_event_type(self, event_store, client):
        await event_store.create_subscription_to_events(
            subscription_name='sub-events',
            event_types=['Wanted', 'AlsoWanted'],
            last_commit_position=9
        )

        kwargs = client.subscribe_to_all.call_args.kwargs
        assert kwargs['commit_position'] == 9
        assert kwargs['filter_include'] == ['Wanted', 'AlsoWanted']
        assert kwargs['filter_by_stream_name'] is False

    @pytest.mark.asyncio
    async def test_create_subscription_to_stream_filters_by_stream_name(self, event_store, client):
        await event_store.create_subscription_to_stream(
            subscription_name='sub-stream',
            stream_name='stream-a',
            last_commit_position=9
        )

        kwargs = client.subscribe_to_all.call_args.kwargs
        assert kwargs['commit_position'] == 9
        assert kwargs['filter_include'] == ['stream-a']
        assert kwargs['filter_by_stream_name'] is True

    @pytest.mark.asyncio
    async def test_created_subscriptions_are_running_and_tracked(self, event_store):
        first = await event_store.create_subscription_to_all(subscription_name='first', last_commit_position=None)
        second = await event_store.create_subscription_to_stream(
            subscription_name='second',
            stream_name='stream-a',
            last_commit_position=None
        )

        assert first.is_running() is True
        assert second.is_running() is True
        assert event_store.get_subscriptions() == (first, second)

    @pytest.mark.asyncio
    async def test_get_subscriptions_returns_empty_tuple_when_none_created(self, event_store):
        assert event_store.get_subscriptions() == ()


class TestKurrentDBEventStoreLifecycle:
    @pytest.mark.asyncio
    async def test_initialize_does_not_touch_the_client(self, event_store, client):
        await event_store.initialize()

        assert client.method_calls == []

    @pytest.mark.asyncio
    async def test_shutdown_with_no_subscriptions(self, event_store):
        await event_store.shutdown()

        assert event_store.get_subscriptions() == ()

    @pytest.mark.asyncio
    async def test_shutdown_stops_all_running_subscriptions(self, event_store, client):
        first_kdb, second_kdb = FakeCatchupSubscription(), FakeCatchupSubscription()
        client.subscribe_to_all.side_effect = [first_kdb, second_kdb]
        first = await event_store.create_subscription_to_all(subscription_name='first', last_commit_position=None)
        second = await event_store.create_subscription_to_all(subscription_name='second', last_commit_position=None)

        await event_store.shutdown()

        assert first.is_running() is False
        assert second.is_running() is False
        assert (first_kdb.stop_call_count, second_kdb.stop_call_count) == (1, 1)

    @pytest.mark.asyncio
    async def test_shutdown_ignores_already_stopped_subscriptions(self, event_store, client):
        kdb_subscription = FakeCatchupSubscription()
        client.subscribe_to_all.return_value = kdb_subscription
        subscription = await event_store.create_subscription_to_all(subscription_name='sub', last_commit_position=None)
        await subscription.stop()

        await event_store.shutdown()

        assert subscription.is_running() is False
        assert kdb_subscription.stop_call_count == 1

    @pytest.mark.asyncio
    async def test_shutdown_keeps_subscriptions_in_the_registry(self, event_store):
        subscription = await event_store.create_subscription_to_all(subscription_name='sub', last_commit_position=None)

        await event_store.shutdown()

        assert event_store.get_subscriptions() == (subscription,)


def test_event_store_exposes_the_wrapped_client(client):
    store = KurrentDBEventStore(client=client)

    assert store.client is client


def test_subscription_type_is_compatible_with_the_client_subscription_protocol():
    """The adapter casts what the client returns to AsyncCatchupSubscription, so the fake must match its shape."""
    for method in ('__aiter__', '__anext__', 'stop'):
        assert hasattr(AsyncCatchupSubscription, method)
        assert hasattr(FakeCatchupSubscription, method)
