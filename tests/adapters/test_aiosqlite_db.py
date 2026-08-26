import asyncio
from dataclasses import FrozenInstanceError
from typing import AsyncIterator

import aiosqlite
import pytest
import pytest_asyncio

from bestagon.adapters.aiosqlite_db import (
    AIOSQLiteEventStore,
    AIOSQLiteEventStoreSubscription,
    AIOSQLiteSubscriptionParameters,
)
from bestagon.core.event_store import (
    ExpectedVersionError,
    NewEventStoreEvent,
    OptimisticConcurrencyError,
    SubscriptionError,
)

POLL_INTERVAL = 0.01
INSERT_SQL = '''
INSERT INTO events (stream_name, stream_position, event_type, payload, metadata)
VALUES (?, ?, ?, ?, ?)
'''


def new_event(event_type: str = 'TestEvent', payload: bytes = b'payload', metadata: bytes = b'metadata') -> NewEventStoreEvent:
    return NewEventStoreEvent(event_type=event_type, payload=payload, metadata=metadata)


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


def subscription(connection: aiosqlite.Connection, **parameters) -> AIOSQLiteEventStoreSubscription:
    parameters.setdefault('poll_interval', POLL_INTERVAL)
    return AIOSQLiteEventStoreSubscription(
        name='sub',
        parameters=AIOSQLiteSubscriptionParameters(**parameters),
        connection=connection
    )


async def row_count(connection: aiosqlite.Connection) -> int:
    async with connection.execute('SELECT COUNT(*) FROM events') as cursor:
        row = await cursor.fetchone()
    return row[0]


async def next_event(subscription: AIOSQLiteEventStoreSubscription, timeout: int | float = 1):
    return await asyncio.wait_for(subscription.next_event(), timeout=timeout)


async def assert_no_more_events(subscription: AIOSQLiteEventStoreSubscription) -> None:
    with pytest.raises(asyncio.TimeoutError):
        await next_event(subscription, timeout=POLL_INTERVAL * 10)


class TestAIOSQLiteSubscriptionParameters:
    def test_defaults_subscribe_to_everything_from_the_beginning(self):
        parameters = AIOSQLiteSubscriptionParameters()

        assert parameters.last_commit_position is None
        assert parameters.event_types == ()
        assert parameters.stream_names == ()
        assert parameters.poll_limit == 100
        assert parameters.poll_interval == 0.5

    def test_parameters_are_immutable(self):
        parameters = AIOSQLiteSubscriptionParameters()

        with pytest.raises(FrozenInstanceError):
            parameters.poll_limit = 1


class TestAIOSQLiteEventStoreSubscription:
    @pytest.mark.asyncio
    async def test_is_running_false_before_start(self, event_store, connection):
        assert subscription(connection).is_running() is False

    @pytest.mark.asyncio
    async def test_start_marks_subscription_as_running(self, event_store, connection):
        sub = subscription(connection)
        await sub.start()
        try:
            assert sub.is_running() is True
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_start_raises_when_already_running(self, event_store, connection):
        sub = subscription(connection)
        await sub.start()
        try:
            with pytest.raises(SubscriptionError):
                await sub.start()
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_stop_marks_subscription_as_not_running(self, event_store, connection):
        sub = subscription(connection)
        await sub.start()
        await sub.stop()

        assert sub.is_running() is False

    @pytest.mark.asyncio
    async def test_stop_raises_when_never_started(self, event_store, connection):
        with pytest.raises(SubscriptionError):
            await subscription(connection).stop()

    @pytest.mark.asyncio
    async def test_stop_raises_when_already_stopped(self, event_store, connection):
        sub = subscription(connection)
        await sub.start()
        await sub.stop()

        with pytest.raises(SubscriptionError):
            await sub.stop()

    @pytest.mark.asyncio
    async def test_restart_after_stop_is_allowed(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(event_type='A1'),), expected_version=None)
        sub = subscription(connection)

        await sub.start()
        assert (await next_event(sub)).event_type == 'A1'
        await sub.stop()

        await event_store.append_events('stream-a', (new_event(event_type='A2'),), expected_version=0)
        await sub.start()
        try:
            assert (await next_event(sub)).event_type == 'A2'
        finally:
            await sub.stop()

    @pytest.mark.asyncio
    async def test_next_event_raises_stop_async_iteration_when_not_running(self, event_store, connection):
        with pytest.raises(StopAsyncIteration):
            await subscription(connection).next_event()

    @pytest.mark.asyncio
    async def test_next_event_raises_stop_async_iteration_after_stop(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='A1'), new_event(event_type='A2')),
            expected_version=None
        )
        sub = subscription(connection)
        await sub.start()
        await next_event(sub)
        await sub.stop()

        with pytest.raises(StopAsyncIteration):
            await sub.next_event()

    @pytest.mark.asyncio
    async def test_delivers_existing_events_in_commit_position_order(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='A1'), new_event(event_type='A2')),
            expected_version=None
        )
        sub = subscription(connection)
        await sub.start()
        try:
            first = await next_event(sub)
            second = await next_event(sub)
        finally:
            await sub.stop()

        assert (first.event_type, first.commit_position, first.stream_position) == ('A1', 1, 0)
        assert (second.event_type, second.commit_position, second.stream_position) == ('A2', 2, 1)
        assert first.stream_name == 'stream-a'
        assert first.payload == b'payload'
        assert first.metadata == b'metadata'

    @pytest.mark.asyncio
    async def test_last_commit_position_none_delivers_the_very_first_event(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(event_type='A1'),), expected_version=None)
        sub = subscription(connection, last_commit_position=None)
        await sub.start()
        try:
            event = await next_event(sub)
        finally:
            await sub.stop()

        assert event.commit_position == 1

    @pytest.mark.asyncio
    async def test_resumes_after_last_commit_position(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='A1'), new_event(event_type='A2'), new_event(event_type='A3')),
            expected_version=None
        )
        sub = subscription(connection, last_commit_position=1)
        await sub.start()
        try:
            event = await next_event(sub)
        finally:
            await sub.stop()

        assert event.event_type == 'A2'
        assert event.commit_position == 2

    @pytest.mark.asyncio
    async def test_never_delivers_the_same_event_twice(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='A1'), new_event(event_type='A2')),
            expected_version=None
        )
        sub = subscription(connection)
        await sub.start()
        try:
            received = [(await next_event(sub)).event_type for _ in range(2)]
            await assert_no_more_events(sub)
        finally:
            await sub.stop()

        assert received == ['A1', 'A2']

    @pytest.mark.asyncio
    async def test_filters_by_event_types(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='Wanted'), new_event(event_type='Unwanted')),
            expected_version=None
        )
        sub = subscription(connection, event_types=['Wanted'])
        await sub.start()
        try:
            event = await next_event(sub)
            await assert_no_more_events(sub)
        finally:
            await sub.stop()

        assert event.event_type == 'Wanted'

    @pytest.mark.asyncio
    async def test_filters_by_stream_names(self, event_store, connection):
        await event_store.append_events('wanted-stream', (new_event(event_type='FromWanted'),), expected_version=None)
        await event_store.append_events('other-stream', (new_event(event_type='FromOther'),), expected_version=None)

        sub = subscription(connection, stream_names=['wanted-stream'])
        await sub.start()
        try:
            event = await next_event(sub)
            await assert_no_more_events(sub)
        finally:
            await sub.stop()

        assert event.stream_name == 'wanted-stream'
        assert event.event_type == 'FromWanted'

    @pytest.mark.asyncio
    async def test_event_type_and_stream_name_filters_are_combined_with_and(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(event_type='Wanted'),), expected_version=None)
        await event_store.append_events('stream-b', (new_event(event_type='Wanted'),), expected_version=None)
        await event_store.append_events('stream-a', (new_event(event_type='Unwanted'),), expected_version=0)

        sub = subscription(connection, event_types=['Wanted'], stream_names=['stream-a'])
        await sub.start()
        try:
            event = await next_event(sub)
            await assert_no_more_events(sub)
        finally:
            await sub.stop()

        assert (event.stream_name, event.event_type) == ('stream-a', 'Wanted')

    @pytest.mark.asyncio
    async def test_picks_up_events_appended_after_start(self, event_store, connection):
        sub = subscription(connection)
        await sub.start()
        try:
            await event_store.append_events('stream-a', (new_event(event_type='Late'),), expected_version=None)
            event = await next_event(sub)
        finally:
            await sub.stop()

        assert event.event_type == 'Late'

    @pytest.mark.asyncio
    async def test_respects_poll_limit_across_multiple_polls(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            tuple(new_event(event_type=f'E{i}') for i in range(3)),
            expected_version=None
        )
        sub = subscription(connection, poll_limit=1)
        await sub.start()
        try:
            events = [await next_event(sub) for _ in range(3)]
        finally:
            await sub.stop()

        assert [event.event_type for event in events] == ['E0', 'E1', 'E2']

    @pytest.mark.asyncio
    async def test_async_iteration_yields_events(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='Iter1'), new_event(event_type='Iter2')),
            expected_version=None
        )
        sub = subscription(connection)
        await sub.start()
        try:
            received = []
            async for event in sub:
                received.append(event.event_type)
                if len(received) == 2:
                    break
        finally:
            await sub.stop()

        assert received == ['Iter1', 'Iter2']

    @pytest.mark.asyncio
    async def test_subscriptions_have_unique_identity(self, event_store, connection):
        first, second = subscription(connection), subscription(connection)

        assert first.id != second.id
        assert first != second
        assert first == first
        assert len({first, second, first}) == 2

    @pytest.mark.asyncio
    async def test_task_stops_and_raises_on_database_error(self, event_store, connection):
        sub = subscription(connection)
        await sub.start()
        task = sub._subscription_task

        async with connection.execute('DROP TABLE events'):
            pass

        with pytest.raises(aiosqlite.Error):
            await asyncio.wait_for(task, timeout=1)

        assert sub.is_running() is False


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
    async def test_initialize_preserves_existing_events(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()
        await store.append_events('stream-a', (new_event(),), expected_version=None)

        await store.initialize()

        assert await row_count(connection) == 1

    @pytest.mark.asyncio
    async def test_append_events_with_empty_tuple_is_noop(self, event_store):
        await event_store.append_events('stream-a', (), expected_version=None)

        assert await event_store.stream_exists('stream-a') is False

    @pytest.mark.asyncio
    async def test_append_events_with_empty_tuple_skips_version_check(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(),), expected_version=None)

        await event_store.append_events('stream-a', (), expected_version=999)

        assert await row_count(connection) == 1

    @pytest.mark.asyncio
    async def test_append_events_rejects_invalid_event_type(self, event_store, connection):
        with pytest.raises(TypeError):
            await event_store.append_events('stream-a', ('not-a-new-stream-event',), expected_version=None)

        assert await row_count(connection) == 0

    @pytest.mark.asyncio
    async def test_append_events_rejects_mixed_valid_and_invalid_events(self, event_store, connection):
        with pytest.raises(TypeError):
            await event_store.append_events('stream-a', (new_event(), object()), expected_version=None)

        assert await row_count(connection) == 0

    @pytest.mark.asyncio
    async def test_append_events_assigns_sequential_positions(self, event_store):
        events = (new_event(event_type='E0'), new_event(event_type='E1'), new_event(event_type='E2'))
        await event_store.append_events('stream-a', events, expected_version=None)

        stream = await event_store.get_stream('stream-a')

        assert [e.event_type for e in stream] == ['E0', 'E1', 'E2']
        assert [e.stream_position for e in stream] == [0, 1, 2]
        assert [e.commit_position for e in stream] == [1, 2, 3]
        assert all(e.stream_name == 'stream-a' for e in stream)
        assert all(e.payload == b'payload' and e.metadata == b'metadata' for e in stream)

    @pytest.mark.asyncio
    async def test_append_events_continues_existing_stream_position(self, event_store):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='E0'), new_event(event_type='E1')),
            expected_version=None
        )
        await event_store.append_events('stream-a', (new_event(event_type='E2'),), expected_version=1)

        stream = await event_store.get_stream('stream-a')

        assert [e.stream_position for e in stream] == [0, 1, 2]
        assert [e.commit_position for e in stream] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_append_events_commit_position_is_global_across_streams(self, event_store):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='A0'), new_event(event_type='A1')),
            expected_version=None
        )
        await event_store.append_events('stream-b', (new_event(event_type='B0'),), expected_version=None)

        stream_a = await event_store.get_stream('stream-a')
        stream_b = await event_store.get_stream('stream-b')

        assert [e.commit_position for e in stream_a] == [1, 2]
        assert [e.commit_position for e in stream_b] == [3]
        assert [e.stream_position for e in stream_b] == [0]

    @pytest.mark.asyncio
    async def test_append_events_raises_when_expected_version_is_stale(self, event_store, connection):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='E0'), new_event(event_type='E1')),
            expected_version=None
        )

        with pytest.raises(ExpectedVersionError):
            await event_store.append_events('stream-a', (new_event(event_type='E2'),), expected_version=0)

        assert await row_count(connection) == 2

    @pytest.mark.asyncio
    async def test_append_events_raises_when_expected_version_is_ahead(self, event_store, connection):
        with pytest.raises(ExpectedVersionError):
            await event_store.append_events('stream-a', (new_event(),), expected_version=0)

        assert await row_count(connection) == 0

    @pytest.mark.asyncio
    async def test_append_events_raises_when_none_expected_but_stream_exists(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(),), expected_version=None)

        with pytest.raises(ExpectedVersionError):
            await event_store.append_events('stream-a', (new_event(),), expected_version=None)

        assert await row_count(connection) == 1

    @pytest.mark.asyncio
    async def test_expected_version_error_reports_both_versions(self, event_store):
        await event_store.append_events('stream-a', (new_event(),), expected_version=None)

        with pytest.raises(ExpectedVersionError) as error:
            await event_store.append_events('stream-a', (new_event(),), expected_version=7)

        assert '"7"' in str(error.value)
        assert '"0"' in str(error.value)

    @pytest.mark.asyncio
    async def test_append_events_raises_optimistic_concurrency_error_on_position_clash(
        self, event_store, connection, monkeypatch
    ):
        await event_store.append_events('stream-a', (new_event(event_type='E0'),), expected_version=None)

        async def stale_version(stream_name: str) -> None:
            """Simulate a concurrent writer that appended after the version was read."""
            return None

        monkeypatch.setattr(event_store, 'get_stream_version', stale_version)

        with pytest.raises(OptimisticConcurrencyError):
            await event_store.append_events('stream-a', (new_event(event_type='Clash'),), expected_version=None)

        assert await row_count(connection) == 1

    @pytest.mark.asyncio
    async def test_append_events_rolls_back_partially_written_batch(self, event_store, connection, monkeypatch):
        await connection.executemany(
            INSERT_SQL,
            [('stream-a', 0, 'E0', b'p', b'm'), ('stream-a', 2, 'E2', b'p', b'm')]
        )
        await connection.commit()

        async def stale_version(stream_name: str) -> int:
            return 0

        monkeypatch.setattr(event_store, 'get_stream_version', stale_version)

        with pytest.raises(OptimisticConcurrencyError):
            await event_store.append_events(
                'stream-a',
                (new_event(event_type='E1'), new_event(event_type='Clash')),
                expected_version=0
            )

        assert await row_count(connection) == 2

    @pytest.mark.asyncio
    async def test_append_events_propagates_unrelated_integrity_errors(self, event_store, connection):
        with pytest.raises(aiosqlite.IntegrityError):
            await event_store.append_events(None, (new_event(),), expected_version=None)

        assert await row_count(connection) == 0

    @pytest.mark.asyncio
    async def test_append_events_concurrent_writes_are_serialized(self, event_store):
        stream_names = [f'stream-{i}' for i in range(5)]

        await asyncio.gather(*(
            event_store.append_events(stream_name, (new_event(), new_event()), expected_version=None)
            for stream_name in stream_names
        ))

        commit_positions = []
        for stream_name in stream_names:
            stream = await event_store.get_stream(stream_name)
            assert [e.stream_position for e in stream] == [0, 1]
            commit_positions.extend(e.commit_position for e in stream)

        assert sorted(commit_positions) == list(range(1, 11))

    @pytest.mark.asyncio
    async def test_concurrent_appends_to_one_stream_allow_only_one_writer(self, event_store, connection):
        await event_store.append_events('stream-a', (new_event(),), expected_version=None)

        results = await asyncio.gather(
            event_store.append_events('stream-a', (new_event(),), expected_version=0),
            event_store.append_events('stream-a', (new_event(),), expected_version=0),
            return_exceptions=True
        )

        assert [type(result) for result in results].count(ExpectedVersionError) == 1
        assert results.count(None) == 1
        assert await row_count(connection) == 2

    @pytest.mark.asyncio
    async def test_get_stream_returns_empty_tuple_for_unknown_stream(self, event_store):
        assert await event_store.get_stream('missing') == ()

    @pytest.mark.asyncio
    async def test_get_stream_returns_only_requested_stream(self, event_store):
        await event_store.append_events('stream-a', (new_event(event_type='A'),), expected_version=None)
        await event_store.append_events('stream-b', (new_event(event_type='B'),), expected_version=None)

        stream = await event_store.get_stream('stream-a')

        assert [e.event_type for e in stream] == ['A']

    @pytest.mark.asyncio
    async def test_get_stream_orders_by_stream_position_regardless_of_insertion_order(self, event_store, connection):
        await connection.executemany(
            INSERT_SQL,
            [
                ('stream-a', 2, 'E2', b'p', b'm'),
                ('stream-a', 0, 'E0', b'p', b'm'),
                ('stream-a', 1, 'E1', b'p', b'm'),
            ]
        )
        await connection.commit()

        stream = await event_store.get_stream('stream-a')

        assert [e.event_type for e in stream] == ['E0', 'E1', 'E2']

    @pytest.mark.asyncio
    async def test_get_stream_version_returns_none_for_unknown_stream(self, event_store):
        assert await event_store.get_stream_version('missing') is None

    @pytest.mark.asyncio
    async def test_get_stream_version_returns_max_stream_position(self, event_store):
        await event_store.append_events('stream-a', (new_event(), new_event(), new_event()), expected_version=None)

        assert await event_store.get_stream_version('stream-a') == 2

    @pytest.mark.asyncio
    async def test_get_stream_version_is_per_stream(self, event_store):
        await event_store.append_events('stream-a', (new_event(), new_event()), expected_version=None)
        await event_store.append_events('stream-b', (new_event(),), expected_version=None)

        assert await event_store.get_stream_version('stream-a') == 1
        assert await event_store.get_stream_version('stream-b') == 0

    @pytest.mark.asyncio
    async def test_stream_exists(self, event_store):
        assert await event_store.stream_exists('stream-a') is False

        await event_store.append_events('stream-a', (new_event(),), expected_version=None)

        assert await event_store.stream_exists('stream-a') is True
        assert await event_store.stream_exists('stream-b') is False

    @pytest.mark.asyncio
    async def test_create_subscription_starts_and_tracks_subscription(self, event_store):
        sub = await event_store.create_subscription(
            subscription_name='sub',
            subscription_parameters=AIOSQLiteSubscriptionParameters(poll_interval=POLL_INTERVAL)
        )

        assert isinstance(sub, AIOSQLiteEventStoreSubscription)
        assert sub.name == 'sub'
        assert sub.is_running() is True
        assert event_store.get_subscriptions() == (sub,)

    @pytest.mark.asyncio
    async def test_create_subscription_to_all_receives_every_event(self, event_store):
        await event_store.append_events('stream-a', (new_event(event_type='A'),), expected_version=None)
        await event_store.append_events('stream-b', (new_event(event_type='B'),), expected_version=None)

        sub = await event_store.create_subscription_to_all(subscription_name='sub-all', last_commit_position=None)

        received = [(await next_event(sub)).event_type for _ in range(2)]

        assert received == ['A', 'B']

    @pytest.mark.asyncio
    async def test_create_subscription_to_all_honours_last_commit_position(self, event_store):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='A'), new_event(event_type='B')),
            expected_version=None
        )

        sub = await event_store.create_subscription_to_all(subscription_name='sub-all', last_commit_position=1)

        event = await next_event(sub)

        assert event.event_type == 'B'

    @pytest.mark.asyncio
    async def test_create_subscription_to_events_filters_by_event_type(self, event_store):
        await event_store.append_events(
            'stream-a',
            (new_event(event_type='Wanted'), new_event(event_type='Unwanted')),
            expected_version=None
        )

        sub = await event_store.create_subscription_to_events(
            subscription_name='sub-events',
            event_types=['Wanted'],
            last_commit_position=None
        )

        event = await next_event(sub)
        await assert_no_more_events(sub)

        assert event.event_type == 'Wanted'

    @pytest.mark.asyncio
    async def test_create_subscription_to_stream_filters_by_stream_name(self, event_store):
        await event_store.append_events('wanted-stream', (new_event(event_type='FromWanted'),), expected_version=None)
        await event_store.append_events('other-stream', (new_event(event_type='FromOther'),), expected_version=None)

        sub = await event_store.create_subscription_to_stream(
            subscription_name='sub-stream',
            stream_name='wanted-stream',
            last_commit_position=None
        )

        event = await next_event(sub)
        await assert_no_more_events(sub)

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
        sub = await store.create_subscription_to_all(subscription_name='first', last_commit_position=None)
        await sub.stop()

        await store.shutdown()

        assert sub.is_running() is False

    @pytest.mark.asyncio
    async def test_shutdown_with_no_subscriptions(self, connection):
        store = AIOSQLiteEventStore(connection=connection)
        await store.initialize()

        await store.shutdown()

        assert store.get_subscriptions() == ()
