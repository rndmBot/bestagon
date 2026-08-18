import asyncio
from typing import AsyncIterator

import aiosqlite
import pytest
import pytest_asyncio

from bestagon.adapters.aiosqlite_db import (
    AIOSQLiteEventStore,
    AIOSQLiteEventStoreSubscription,
    AIOSQLiteSubscriptionParameters,
)
from bestagon.core.event_store import NewStreamEvent, SubscriptionError

POLL_INTERVAL = 0.01


def new_event(event_type: str = 'TestEvent', payload: bytes = b'payload', metadata: bytes = b'metadata') -> NewStreamEvent:
    return NewStreamEvent(event_type=event_type, payload=payload, metadata=metadata)


@pytest_asyncio.fixture
async def connection() -> AsyncIterator[aiosqlite.Connection]:
    conn = await aiosqlite.connect(':memory:')
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def event_store(connection: aiosqlite.Connection) -> AsyncIterator[AIOSQLiteEventStore]:
    store = AIOSQLiteEventStore(connection=connection)
    await store.initialize()
    try:
        yield store
    finally:
        await store.shutdown()


async def row_count(connection: aiosqlite.Connection) -> int:
    async with connection.execute('SELECT COUNT(*) FROM events') as cursor:
        row = await cursor.fetchone()
    return row[0]


class TestAIOSQLiteEventStoreSubscription:
    @pytest.mark.asyncio
    async def test_is_running_false_before_start(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(), connection=connection
        )
        assert subscription.is_running() is False

    @pytest.mark.asyncio
    async def test_start_marks_subscription_as_running(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        try:
            assert subscription.is_running() is True
        finally:
            await subscription.stop()

    @pytest.mark.asyncio
    async def test_start_raises_when_already_running(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        try:
            with pytest.raises(SubscriptionError):
                await subscription.start()
        finally:
            await subscription.stop()

    @pytest.mark.asyncio
    async def test_stop_marks_subscription_as_not_running(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        await subscription.stop()
        assert subscription.is_running() is False

    @pytest.mark.asyncio
    async def test_stop_raises_when_not_running(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(), connection=connection
        )
        with pytest.raises(SubscriptionError):
            await subscription.stop()

    @pytest.mark.asyncio
    async def test_stop_raises_when_already_stopped(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        await subscription.stop()
        with pytest.raises(SubscriptionError):
            await subscription.stop()

    @pytest.mark.asyncio
    async def test_next_event_raises_stop_async_iteration_when_not_running(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(), connection=connection
        )
        with pytest.raises(StopAsyncIteration):
            await subscription.next_event()

    @pytest.mark.asyncio
    async def test_delivers_existing_events_in_commit_position_order(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(event_type='A1'), new_event(event_type='A2')))

        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        try:
            first = await asyncio.wait_for(subscription.next_event(), timeout=1)
            second = await asyncio.wait_for(subscription.next_event(), timeout=1)
        finally:
            await subscription.stop()

        assert (first.event_type, first.commit_position, first.stream_position) == ('A1', 0, 0)
        assert (second.event_type, second.commit_position, second.stream_position) == ('A2', 1, 1)
        assert first.stream_name == 'stream-a'
        assert first.payload == b'payload'
        assert first.metadata == b'metadata'

    @pytest.mark.asyncio
    async def test_resumes_after_last_commit_position(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(event_type='A1'), new_event(event_type='A2'), new_event(event_type='A3')))

        subscription = AIOSQLiteEventStoreSubscription(
            name='sub',
            parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL, last_commit_position=0),
            connection=connection,
        )
        await subscription.start()
        try:
            event = await asyncio.wait_for(subscription.next_event(), timeout=1)
        finally:
            await subscription.stop()

        assert event.event_type == 'A2'
        assert event.commit_position == 1

    @pytest.mark.asyncio
    async def test_filters_by_event_types(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(event_type='Wanted'), new_event(event_type='Unwanted')))

        subscription = AIOSQLiteEventStoreSubscription(
            name='sub',
            parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL, event_types=['Wanted']),
            connection=connection,
        )
        await subscription.start()
        try:
            event = await asyncio.wait_for(subscription.next_event(), timeout=1)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(subscription.next_event(), timeout=0.1)
        finally:
            await subscription.stop()

        assert event.event_type == 'Wanted'

    @pytest.mark.asyncio
    async def test_filters_by_stream_names(self, event_store, connection):
        await event_store.append_events('wanted-stream', (new_event(event_type='FromWanted'),))
        await event_store.append_events('other-stream', (new_event(event_type='FromOther'),))

        subscription = AIOSQLiteEventStoreSubscription(
            name='sub',
            parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL, stream_names=['wanted-stream']),
            connection=connection,
        )
        await subscription.start()
        try:
            event = await asyncio.wait_for(subscription.next_event(), timeout=1)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(subscription.next_event(), timeout=0.1)
        finally:
            await subscription.stop()

        assert event.stream_name == 'wanted-stream'
        assert event.event_type == 'FromWanted'

    @pytest.mark.asyncio
    async def test_picks_up_events_appended_after_start(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        try:
            await event_store.append_events('stream-a', (new_event(event_type='Late'),))
            event = await asyncio.wait_for(subscription.next_event(), timeout=1)
        finally:
            await subscription.stop()

        assert event.event_type == 'Late'

    @pytest.mark.asyncio
    async def test_respects_poll_limit_across_multiple_polls(self, event_store, connection):
        await event_store.append_events(
            'stream-a', tuple(new_event(event_type=f'E{i}') for i in range(3))
        )

        subscription = AIOSQLiteEventStoreSubscription(
            name='sub',
            parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL, poll_limit=1),
            connection=connection,
        )
        await subscription.start()
        try:
            events = [await asyncio.wait_for(subscription.next_event(), timeout=1) for _ in range(3)]
        finally:
            await subscription.stop()

        assert [event.event_type for event in events] == ['E0', 'E1', 'E2']

    @pytest.mark.asyncio
    async def test_async_iteration_yields_events(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(event_type='Iter1'), new_event(event_type='Iter2')))

        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        try:
            received = []
            async for event in subscription:
                received.append(event.event_type)
                if len(received) == 2:
                    break
        finally:
            await subscription.stop()

        assert received == ['Iter1', 'Iter2']

    @pytest.mark.asyncio
    async def test_task_stops_and_raises_on_database_error(self, event_store, connection):
        subscription = AIOSQLiteEventStoreSubscription(
            name='sub', parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL), connection=connection
        )
        await subscription.start()
        task = subscription._subscription_task

        async with connection.execute('DROP TABLE events'):
            pass

        with pytest.raises(aiosqlite.Error):
            await asyncio.wait_for(task, timeout=1)

        assert subscription.is_running() is False


class TestAIOSQLiteEventStore:
    @pytest.mark.asyncio
    async def test_initialize_creates_events_table(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()

        async with connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
        ) as cursor:
            row = await cursor.fetchone()
        assert row is not None

    @pytest.mark.asyncio
    async def test_initialize_creates_expected_indexes(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()

        async with connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='events'"
        ) as cursor:
            rows = await cursor.fetchall()
        index_names = {row[0] for row in rows}

        assert {'events_event_type_index', 'events_stream_name_index', 'events_stream_position_index'} <= index_names

    @pytest.mark.asyncio
    async def test_initialize_is_idempotent(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()
        await store.initialize()

        async with connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
        ) as cursor:
            rows = await cursor.fetchall()
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_append_events_with_empty_tuple_is_noop(self, event_store):
        await event_store.append_events('stream-a', ())
        assert await event_store.stream_exists('stream-a') is False

    @pytest.mark.asyncio
    async def test_append_events_rejects_invalid_event_type(self, event_store):
        with pytest.raises(TypeError):
            await event_store.append_events('stream-a', ('not-a-new-stream-event',))

    @pytest.mark.asyncio
    async def test_append_events_rejects_mixed_valid_and_invalid_events(self, event_store):
        with pytest.raises(TypeError):
            await event_store.append_events('stream-a', (new_event(), object()))

    @pytest.mark.asyncio
    async def test_append_events_assigns_sequential_positions(self, event_store):
        events = (new_event(event_type='E0'), new_event(event_type='E1'), new_event(event_type='E2'))
        await event_store.append_events('stream-a', events)

        stream = await event_store.get_stream('stream-a')

        assert [e.event_type for e in stream] == ['E0', 'E1', 'E2']
        assert [e.stream_position for e in stream] == [0, 1, 2]
        assert [e.commit_position for e in stream] == [0, 1, 2]
        assert all(e.stream_name == 'stream-a' for e in stream)
        assert all(e.payload == b'payload' and e.metadata == b'metadata' for e in stream)

    @pytest.mark.asyncio
    async def test_append_events_continues_existing_stream_position(self, event_store):
        await event_store.append_events('stream-a', (new_event(event_type='E0'), new_event(event_type='E1')))
        await event_store.append_events('stream-a', (new_event(event_type='E2'),))

        stream = await event_store.get_stream('stream-a')

        assert [e.stream_position for e in stream] == [0, 1, 2]
        assert [e.commit_position for e in stream] == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_append_events_commit_position_is_global_across_streams(self, event_store):
        await event_store.append_events('stream-a', (new_event(event_type='A0'), new_event(event_type='A1')))
        await event_store.append_events('stream-b', (new_event(event_type='B0'),))

        stream_a = await event_store.get_stream('stream-a')
        stream_b = await event_store.get_stream('stream-b')

        assert [e.commit_position for e in stream_a] == [0, 1]
        assert [e.commit_position for e in stream_b] == [2]
        assert [e.stream_position for e in stream_b] == [0]

    @pytest.mark.asyncio
    async def test_append_events_rolls_back_on_integrity_error(self, event_store, connection, monkeypatch):
        await event_store.append_events('stream-a', (new_event(),))

        async def colliding_commit_position() -> int:
            return 0

        monkeypatch.setattr(event_store, '_get_next_commit_position', colliding_commit_position)

        with pytest.raises(aiosqlite.IntegrityError):
            await event_store.append_events('stream-b', (new_event(),))

        assert await event_store.stream_exists('stream-b') is False
        assert await row_count(connection) == 1

    @pytest.mark.asyncio
    async def test_get_stream_returns_empty_tuple_for_unknown_stream(self, event_store):
        assert await event_store.get_stream('missing') == ()

    @pytest.mark.asyncio
    async def test_get_stream_orders_by_stream_position_regardless_of_insertion_order(self, event_store, connection):
        insert_sql = '''
            INSERT INTO events (commit_position, stream_name, stream_position, event_type, payload, metadata)
            VALUES (:commit_position, :stream_name, :stream_position, :event_type, :payload, :metadata)
        '''
        rows = [
            {'commit_position': 2, 'stream_name': 'stream-a', 'stream_position': 2, 'event_type': 'E2', 'payload': b'p', 'metadata': b'm'},
            {'commit_position': 0, 'stream_name': 'stream-a', 'stream_position': 0, 'event_type': 'E0', 'payload': b'p', 'metadata': b'm'},
            {'commit_position': 1, 'stream_name': 'stream-a', 'stream_position': 1, 'event_type': 'E1', 'payload': b'p', 'metadata': b'm'},
        ]
        async with connection.cursor() as cursor:
            for row in rows:
                await cursor.execute(insert_sql, row)
        await connection.commit()

        stream = await event_store.get_stream('stream-a')

        assert [e.event_type for e in stream] == ['E0', 'E1', 'E2']

    @pytest.mark.asyncio
    async def test_get_stream_version_returns_none_for_unknown_stream(self, event_store):
        assert await event_store.get_stream_version('missing') is None

    @pytest.mark.asyncio
    async def test_get_stream_version_returns_max_stream_position(self, event_store):
        await event_store.append_events('stream-a', (new_event(), new_event(), new_event()))
        assert await event_store.get_stream_version('stream-a') == 2

    @pytest.mark.asyncio
    async def test_stream_exists(self, event_store):
        assert await event_store.stream_exists('stream-a') is False
        await event_store.append_events('stream-a', (new_event(),))
        assert await event_store.stream_exists('stream-a') is True

    @pytest.mark.asyncio
    async def test_create_subscription_starts_and_tracks_subscription(self, event_store):
        subscription = await event_store.create_subscription(
            subscription_name='sub', subscription_parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL)
        )

        assert subscription.is_running() is True
        assert event_store.get_subscriptions() == (subscription,)

    @pytest.mark.asyncio
    async def test_create_subscription_to_all_receives_every_event(self, event_store):
        await event_store.append_events('stream-a', (new_event(event_type='A'),))
        await event_store.append_events('stream-b', (new_event(event_type='B'),))

        subscription = await event_store.create_subscription_to_all(subscription_name='sub-all', last_commit_position=None)

        first = await asyncio.wait_for(subscription.next_event(), timeout=1)
        second = await asyncio.wait_for(subscription.next_event(), timeout=1)

        assert {first.event_type, second.event_type} == {'A', 'B'}

    @pytest.mark.asyncio
    async def test_create_subscription_to_events_filters_by_event_type(self, event_store):
        await event_store.append_events('stream-a', (new_event(event_type='Wanted'), new_event(event_type='Unwanted')))

        subscription = await event_store.create_subscription_to_events(
            subscription_name='sub-events', event_types=['Wanted'], last_commit_position=None
        )

        event = await asyncio.wait_for(subscription.next_event(), timeout=1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(subscription.next_event(), timeout=0.1)

        assert event.event_type == 'Wanted'

    @pytest.mark.asyncio
    async def test_create_subscription_to_stream_filters_by_stream_name(self, event_store):
        await event_store.append_events('wanted-stream', (new_event(event_type='FromWanted'),))
        await event_store.append_events('other-stream', (new_event(event_type='FromOther'),))

        subscription = await event_store.create_subscription_to_stream(
            subscription_name='sub-stream', stream_name='wanted-stream', last_commit_position=None
        )

        event = await asyncio.wait_for(subscription.next_event(), timeout=1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(subscription.next_event(), timeout=0.1)

        assert event.stream_name == 'wanted-stream'

    @pytest.mark.asyncio
    async def test_get_subscriptions_returns_all_created_subscriptions_in_order(self, event_store):
        first = await event_store.create_subscription_to_all(subscription_name='first', last_commit_position=None)
        second = await event_store.create_subscription_to_all(subscription_name='second', last_commit_position=None)

        assert event_store.get_subscriptions() == (first, second)

    @pytest.mark.asyncio
    async def test_get_subscriptions_returns_empty_tuple_when_none_created(self, event_store):
        assert event_store.get_subscriptions() == ()

    @pytest.mark.asyncio
    async def test_shutdown_stops_all_running_subscriptions(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()
        first = await store.create_subscription_to_all(subscription_name='first', last_commit_position=None)
        second = await store.create_subscription_to_all(subscription_name='second', last_commit_position=None)

        await store.shutdown()

        assert first.is_running() is False
        assert second.is_running() is False

    @pytest.mark.asyncio
    async def test_shutdown_ignores_already_stopped_subscriptions(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()
        subscription = await store.create_subscription_to_all(subscription_name='first', last_commit_position=None)
        await subscription.stop()

        await store.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_with_no_subscriptions(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()

        await store.shutdown()

    @pytest.mark.asyncio
    async def test_append_events_concurrent_writes_are_serialized(self, event_store):
        stream_names = [f'stream-{i}' for i in range(5)]

        await asyncio.gather(*(
            event_store.append_events(stream_name, (new_event(), new_event()))
            for stream_name in stream_names
        ))

        commit_positions = []
        for stream_name in stream_names:
            stream = await event_store.get_stream(stream_name)
            assert [e.stream_position for e in stream] == [0, 1]
            commit_positions.extend(e.commit_position for e in stream)

        assert sorted(commit_positions) == list(range(10))
