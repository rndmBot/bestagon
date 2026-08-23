import asyncio
import logging
from abc import abstractmethod
from asyncio import Queue
from dataclasses import dataclass
from sqlite3 import IntegrityError
from typing import Tuple, List, Union, Sequence

import aiosqlite
from bestagon.core.checkpoint_store import CheckpointStore, Checkpoint
from bestagon.core.event_processor import Projection
from bestagon.core.event_store import EventStore, NewStreamEvent, StreamEvent, SubscriptionParameters, \
    EventStoreSubscription, SubscriptionError, ExpectedVersionError, OptimisticConcurrencyError

logger = logging.getLogger(__name__)


class AIOSQLiteCheckpointStore(CheckpointStore):
    """
    Ready to use Checkpoint Store implementation that uses `aiosqlite` as a storage.
    """

    def __init__(self, connection: aiosqlite.Connection):
        self._connection = connection

    async def close(self) -> None:
        pass

    async def delete_checkpoint(self, name: str) -> None:
        sql = '''
        DELETE FROM _checkpoints
        WHERE name = :name
        '''

        props = {'name': name}
        async with self._connection.cursor() as cursor:
            try:
                await cursor.execute(sql, props)
                await self._connection.commit()
            except Exception as e:
                await self._connection.rollback()
                logger.exception(e)
                raise e
        logger.debug(f'Checkpoint {name} deleted')

    async def get_checkpoint(self, name: str) -> Checkpoint:
        sql = '''
        SELECT *
        FROM _checkpoints
        WHERE name = :name
        '''

        props = {'name': name}
        async with self._connection.cursor() as cursor:
            await cursor.execute(sql, props)
            values = await cursor.fetchone()

            if values:
                columns = [datum[0] for datum in cursor.description]
                data = dict(zip(columns, values))
                checkpoint = Checkpoint(**data)
            else:
                checkpoint = Checkpoint(name=name, value=None)

        return checkpoint

    async def initialize(self) -> None:
        sql = '''
        CREATE TABLE IF NOT EXISTS _checkpoints(
            name TEXT PRIMARY KEY UNIQUE, 
            value INT
        )
        '''
        async with self._connection.cursor() as cursor:
            try:
                await cursor.execute(sql)
                await cursor.close()
                await self._connection.commit()
            except Exception as e:
                await self._connection.rollback()
                raise e
        logger.debug(f'{self.__class__.__qualname__} initialized')

    async def list_checkpoints(self) -> Tuple[Checkpoint, ...]:
        sql = '''
        SELECT * FROM _checkpoints
        '''
        async with self._connection.cursor() as cursor:
            await cursor.execute(sql)
            rows = await cursor.fetchall()

            checkpoints = list()
            if rows:
                columns = [datum[0] for datum in cursor.description]
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    checkpoint = Checkpoint(**row_dict)
                    checkpoints.append(checkpoint)
        return tuple(checkpoints)

    async def set_checkpoint(self, checkpoint: Checkpoint) -> None:
        sql = '''
            INSERT INTO _checkpoints (name, value)
            VALUES (:name, :value)
            ON CONFLICT(name) 
            DO UPDATE SET value = :value
        '''
        params = {'name': checkpoint.name, 'value': checkpoint.value}
        async with self._connection.cursor() as cursor:
            try:
                await cursor.execute(sql, params)
                await self._connection.commit()
            except Exception as e:
                await self._connection.rollback()
                raise e
        logger.debug(f'New checkpoint set {checkpoint.name}: {checkpoint.value}')


@dataclass(frozen=True)
class AIOSQLiteSubscriptionParameters(SubscriptionParameters):
    """
    Subscription parameters for AIOSQLite subscription.

    :param last_commit_position: the commit position of the last consumed event, the subscription will only return events
    after that position
    :param event_types: list of event types to return, the subscription will skip all other events.
    :param stream_names: list of stream names to subscribe to, the subscription will return only events from these streams.
    :param poll_limit: how many events will be polled at once
    :param poll_interval: interval between polls in seconds
    """
    last_commit_position: int | None = None
    event_types: Sequence[str] = ()
    stream_names: Sequence[str] = ()
    poll_limit: int = 100
    poll_interval: int |float = 0.5


class AIOSQLiteEventStoreSubscription(EventStoreSubscription):
    def __init__(self, name: str, parameters: AIOSQLiteSubscriptionParameters, connection: aiosqlite.Connection):
        super().__init__(name=name)
        self._connection = connection

        self._parameters = parameters
        self._last_commit_position = parameters.last_commit_position

        self._consume_events = True
        self._event_queue = Queue(maxsize=1000)
        self._subscription_task: Union[asyncio.Task, None] = None

    def _get_next_commit_position(self) -> int:
        if self._last_commit_position is None:
            return 0
        return self._last_commit_position + 1

    async def _start_subscription_task(self) -> None:
        cursor = await self._connection.cursor()
        try:
            while self._consume_events:
                filters = list()
                params = list()

                # Commit position
                filters.append('commit_position >= ?')
                params.append(self._get_next_commit_position())

                if self._parameters.event_types:
                    placeholders = ",".join(["?"] * len(self._parameters.event_types))
                    filter_string = f'event_type IN ({placeholders})'
                    filters.append(filter_string)
                    params.extend(self._parameters.event_types)
                if self._parameters.stream_names:
                    placeholders = ",".join(["?"] * len(self._parameters.stream_names))
                    filter_string = f'stream_name IN ({placeholders})'
                    filters.append(filter_string)
                    params.extend(self._parameters.stream_names)

                where_filter = ' AND '.join(filters)
                where_filter = f'WHERE {where_filter}'

                sql = f'''
                    SELECT *
                    FROM events
                    {where_filter}
                    ORDER BY commit_position ASC
                    LIMIT ?
                '''
                params.append(self._parameters.poll_limit)

                await cursor.execute(sql, parameters=params)
                rows = await cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    for row in rows:
                        data = dict(zip(columns, row))
                        event = StreamEvent(**data)
                        await self._event_queue.put(event)
                        self._last_commit_position = event.commit_position
                await asyncio.sleep(self._parameters.poll_interval)
        except Exception as e:
            logger.exception(f'Subscription {self.id} failed: {e}')
            await cursor.close()
            raise e
        await cursor.close()

    def is_running(self) -> bool:
        if self._subscription_task is None:
            return False
        else:
            return not self._subscription_task.done()

    async def next_event(self) -> StreamEvent:
        if not self.is_running():
            raise StopAsyncIteration

        next_event = await self._event_queue.get()
        self._event_queue.task_done()
        return next_event

    async def start(self) -> None:
        if self.is_running():
            raise SubscriptionError(f'Subscription {self.name} already started')
        self._consume_events = True
        self._subscription_task = asyncio.create_task(self._start_subscription_task())
        logger.debug(f'Subscription "{self.name}" with ID {self.id} started.')

    async def stop(self) -> None:
        if not self.is_running():
            raise SubscriptionError(f'Subscription {self.name} is already stopped')
        self._consume_events = False
        if self._subscription_task is not None:
            await self._subscription_task
            self._subscription_task = None
        logger.info(f'Subscription {self.name} with ID {self.id} stopped')


class AIOSQLiteEventStore(EventStore):
    def __init__(self, connection: aiosqlite.Connection):
        super().__init__()
        self._connection = connection
        self._subscriptions: List[AIOSQLiteEventStoreSubscription] = list()
        self._write_lock = asyncio.Lock()

    async def _initialize_database(self) -> None:
        logger.info(f'Initializing {self.__class__.__qualname__}')
        create_sql = '''
        CREATE TABLE IF NOT EXISTS events (
            commit_position INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_name VARCHAR NOT NULL,
            stream_position INT NOT NULL,
            event_type VARCHAR NOT NULL,
            payload BLOB NOT NULL,
            metadata BLOB NOT NULL,
            unique(stream_name, stream_position)
        )
        '''
        index_sql = 'CREATE INDEX IF NOT EXISTS events_event_type_index ON events (event_type)'
        stream_name_index_sql = 'CREATE INDEX IF NOT EXISTS events_stream_name_index ON events (stream_name)'
        stream_position_index_sql = 'CREATE INDEX IF NOT EXISTS events_stream_position_index ON events (stream_position)'

        async with self._connection.cursor() as cursor:
            await cursor.execute(create_sql)
            await cursor.execute(index_sql)
            await cursor.execute(stream_name_index_sql)
            await cursor.execute(stream_position_index_sql)
            await self._connection.commit()
        logger.info(f'{self.__class__.__qualname__} initialized')

    async def append_events(
            self,
            stream_name: str,
            events: Tuple[NewStreamEvent, ...],
            expected_version: int | None
    ) -> None:
        if not events:
            return
        if not all(isinstance(e, NewStreamEvent) for e in events):
            raise TypeError(
                f'Failed to append events into {self.__class__.__qualname__}, '
                f'all events must be instances of NewStreamEvent class, '
                f'one or more events are of invalid type, please check types of the passed events.'
            )

        async with self._write_lock:
            logger.debug(f'Appending {len(events)} events into "{stream_name}" stream of {self.__class__.__qualname__}')
            try:
                current_version = await self.get_stream_version(stream_name=stream_name)
                if expected_version != current_version:
                    raise ExpectedVersionError(expected_version, current_version)

                next_stream_position = 0 if current_version is None else current_version + 1
                data = list()
                for event in events:
                    datum = {
                        'stream_name': stream_name,
                        'stream_position': next_stream_position,
                        'event_type': event.event_type,
                        'payload': event.payload,
                        'metadata': event.metadata
                    }
                    data.append(datum)
                    next_stream_position += 1

                sql = '''
                INSERT INTO events (stream_name, stream_position, event_type, payload, metadata)
                VALUES (:stream_name, :stream_position, :event_type, :payload, :metadata)
                '''

                async with self._connection.cursor() as cursor:
                    await cursor.executemany(sql, parameters=data)
                    await self._connection.commit()
                logger.debug(f'New events appended to "{stream_name}" stream of {self.__class__.__qualname__}')
            except ExpectedVersionError as e:
                await self._connection.rollback()
                raise e
            except IntegrityError as e:
                await self._connection.rollback()
                if 'UNIQUE constraint failed' in str(e):
                    raise OptimisticConcurrencyError(
                        f'Failed to append events to event store: one or more events already '
                        f'exists in the provided stream position'
                    ) from e
                else:
                    raise e
            except Exception as e:
                await self._connection.rollback()
                logger.exception(e)
                raise e

    async def create_subscription(self, subscription_name: str, subscription_parameters: 'AIOSQLiteSubscriptionParameters') -> 'AIOSQLiteEventStoreSubscription':
        logger.debug(f'Creating subscription to {self.__class__.__qualname__} with parameters {subscription_parameters}')
        subscription = AIOSQLiteEventStoreSubscription(
            name=subscription_name,
            parameters=subscription_parameters,
            connection=self._connection
        )
        await subscription.start()
        self._subscriptions.append(subscription)
        return subscription

    async def create_subscription_to_all(self, subscription_name: str,
                                         last_commit_position: int | None) -> 'AIOSQLiteEventStoreSubscription':
        parameters = AIOSQLiteSubscriptionParameters(last_commit_position=last_commit_position)
        subscription = await self.create_subscription(subscription_name=subscription_name, subscription_parameters=parameters)
        return subscription

    async def create_subscription_to_events(self, subscription_name: str, event_types: List[str],
                                            last_commit_position: int | None) -> 'AIOSQLiteEventStoreSubscription':
        parameters = AIOSQLiteSubscriptionParameters(
            last_commit_position=last_commit_position,
            event_types=event_types
        )
        subscription = await self.create_subscription(
            subscription_name=subscription_name,
            subscription_parameters=parameters
        )
        return subscription

    async def create_subscription_to_stream(self, subscription_name: str, stream_name: str,
                                            last_commit_position: int | None) -> 'AIOSQLiteEventStoreSubscription':
        parameters = AIOSQLiteSubscriptionParameters(
            last_commit_position=last_commit_position,
            stream_names=[stream_name]
        )
        subscription = await self.create_subscription(subscription_name=subscription_name, subscription_parameters=parameters)
        return subscription

    async def get_stream(self, stream_name: str) -> Tuple[StreamEvent, ...]:
        sql = '''
        SELECT *
        FROM events
        WHERE stream_name = :stream_name
        ORDER BY stream_position ASC
        '''

        async with self._connection.cursor() as cursor:
            params = {'stream_name': stream_name}
            await cursor.execute(sql, parameters=params)
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]

            events = list()
            for row in rows:
                datum = dict(zip(columns, row))
                event = StreamEvent(**datum)
                events.append(event)
        return tuple(events)

    async def get_stream_version(self, stream_name: str) -> int | None:
        sql = '''
        SELECT MAX(stream_position) AS stream_version
        FROM events
        WHERE stream_name = :stream_name
        '''
        async with self._connection.cursor() as cursor:
            params = {'stream_name': stream_name}
            await cursor.execute(sql, parameters=params)
            result = await cursor.fetchone()
        return result[0]

    def get_subscriptions(self) -> Tuple[AIOSQLiteEventStoreSubscription, ...]:
        return tuple(self._subscriptions)

    async def initialize(self) -> None:
        await self._initialize_database()

    async def shutdown(self) -> None:
        logger.info(f'Shuting down {self.__class__.__qualname__}')
        await asyncio.gather(*[sub.stop() for sub in self._subscriptions if sub.is_running()])
        logger.info(f'{self.__class__.__qualname__} shut down')

    async def stream_exists(self, stream_name: str) -> bool:
        sql = '''
        SELECT EXISTS(
            SELECT 1 FROM events WHERE stream_name = :stream_name
        )
        '''

        async with self._connection.cursor() as cursor:
            params = {'stream_name': stream_name}
            await cursor.execute(sql, parameters=params)
            row = await cursor.fetchone()
        return bool(row[0])


class AIOSQLiteProjection(Projection):
    """
    A concrete implementation of Projection that uses `aiosqlite` library as a storage.

    To use this projection yu must reimplement abstract methods:
        - `get_database_name` to provide the name of the table you will store your data.
        - `initialize` - to initialize connection to database and create the necessary table(s) for your projection.

    NOTE:
        If your projection involves multiple tables, them you also need to reimplement `drop` method to drop data from multiple tables.
    """

    def __init__(self, connection: aiosqlite.Connection, checkpoint_store: CheckpointStore):
        super().__init__(checkpoint_store=checkpoint_store)
        self._connection = connection

    @property
    def connection(self) -> aiosqlite.Connection:
        return self._connection

    @property
    def database_name(self) -> str:
        return self.get_database_name()

    async def drop(self) -> None:
        cursor = await self.connection.cursor()
        sql = f'DELETE FROM {self.database_name}'
        try:
            await cursor.execute(sql)
            await cursor.close()
            await self.connection.commit()
        except Exception as e:
            await cursor.close()
            await self.connection.rollback()
            logger.exception(e)
            raise e

    @abstractmethod
    def get_database_name(self) -> str:
        raise NotImplementedError

    async def stop(self) -> None:
        await super().stop()
