import asyncio
import logging
from abc import abstractmethod
from asyncio import Queue
from dataclasses import dataclass
from typing import Tuple, List, Union, Sequence

import aiosqlite
from bestagon.core.checkpoint_store import CheckpointStore, Checkpoint
from bestagon.core.event_processor import Projection
from bestagon.core.event_store import EventStore, NewStreamEvent, StreamEvent, SubscriptionParameters, \
    EventStoreSubscription, SubscriptionError

logger = logging.getLogger(__name__)


class AIOSQLiteCheckpointStore(CheckpointStore):
    """
    Ready to use Checkpoint Store implementation that uses `aiosqlite` as a storage.
    """

    def __init__(self, database: str):
        self._database = database
        self._connection: aiosqlite.Connection = None

    @property
    def connection(self) -> aiosqlite.Connection:
        return self._connection

    @property
    def database(self) -> str:
        return self._database

    async def close(self) -> None:
        await self.connection.close()
        logger.debug(f'Checkpoint store closed.')

    async def delete_checkpoint(self, name: str) -> None:
        sql = '''
        DELETE FROM _checkpoints
        WHERE name = :name
        '''

        props = {'name': name}
        cursor = await self.connection.cursor()
        await cursor.execute(sql, props)
        await cursor.close()
        await self.connection.commit()
        logger.debug(f'Checkpoint {name} deleted')

    async def get_checkpoint(self, name: str) -> Checkpoint:
        sql = '''
        SELECT *
        FROM _checkpoints
        WHERE name = :name
        '''

        props = {'name': name}
        cursor = await self.connection.cursor()
        await cursor.execute(sql, props)
        values = await cursor.fetchone()

        if values:
            columns = [datum[0] for datum in cursor.description]
            data = dict(zip(columns, values))
            checkpoint = Checkpoint(**data)
        else:
            checkpoint = Checkpoint(name=name, value=0)

        await cursor.close()
        return checkpoint

    async def initialize(self) -> None:
        self._connection = await aiosqlite.connect(self.database)

        sql = '''
        CREATE TABLE IF NOT EXISTS _checkpoints(
            name TEXT PRIMARY KEY UNIQUE, 
            value INT
        )
        '''
        cursor = await self.connection.cursor()
        await cursor.execute(sql)
        await cursor.close()
        await self.connection.commit()
        logger.debug(f'{self.__class__.__qualname__} initialized')

    async def list_checkpoints(self) -> Tuple[Checkpoint, ...]:
        sql = '''
        SELECT * FROM _checkpoints
        '''
        cursor = await self.connection.cursor()
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
        cursor = await self.connection.cursor()
        await cursor.execute(sql, params)
        await cursor.close()
        await self.connection.commit()
        logger.debug(f'New checkpoint set {checkpoint.name}: {checkpoint.value}')


@dataclass(frozen=True)
class AIOSQLiteSubscriptionParameters(SubscriptionParameters):
    commit_position: Union[int, None] = None
    event_types: Sequence[str] = ()
    stream_names: Sequence[str] = ()
    fetch_limit: int = 100
    fetch_interval: int = 5


class AIOSQLiteEventStoreSubscription(EventStoreSubscription):
    def __init__(self, name: str, parameters: AIOSQLiteSubscriptionParameters, connection: aiosqlite.Connection):
        super().__init__(name=name)
        self._connection = connection

        self._parameters = parameters
        self._commit_position = parameters.commit_position

        self._running = False
        self._event_queue = Queue()
        self._subscription_task: Union[asyncio.Task, None] = None

    async def _start_subscription_task(self) -> None:
        # TODO - exception handling
        # TODO - logs
        while self.is_running():
            filters = list()
            params = list()
            if self._commit_position:
                filters.append('commit_position > ?')
                params.append(self._commit_position)
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

            if filters:
                where_filter = ' AND '.join(filters)
                where_filter = f'WHERE {where_filter}'
            else:
                where_filter = ''

            sql = f'''
                SELECT *
                FROM events
                {where_filter}
                LIMIT ? 
            '''
            params.append(self._parameters.fetch_limit)

            async with self._connection.cursor() as cursor:
                await cursor.execute(sql, parameters=params)
                rows = await cursor.fetchall()
                if rows:
                    columns = [d[0] for d in cursor.description]
                    for row in rows:
                        data = dict(zip(columns, row))
                        event = StreamEvent(**data)
                        self._event_queue.put_nowait(event)
                        self._commit_position = event.commit_position
            await asyncio.sleep(self._parameters.fetch_interval)

    def is_running(self) -> bool:
        return self._running

    async def next_event(self) -> StreamEvent:
        if not self.is_running():
            raise StopAsyncIteration

        next_event = await self._event_queue.get()
        self._event_queue.task_done()
        return next_event

    async def start(self) -> None:
        # TODO - logs
        if self.is_running():
            raise SubscriptionError(f'Subscription {self.name} already started')
        self._running = True

        if self._subscription_task is None:
            self._subscription_task = asyncio.create_task(self._start_subscription_task())

    async def stop(self) -> None:
        # TODO - logs
        if not self.is_running():
            raise SubscriptionError(f'Subscription {self.name} is already stopped')

        self._running = False
        if self._subscription_task is not None:
            self._subscription_task.cancel()
            self._subscription_task = None


class AIOSQLiteEventStore(EventStore):
    # TODO - log everything

    @dataclass(frozen=True)
    class NewEvent:
        commit_position: int
        stream_name: str
        stream_position: int
        event_type: str
        payload: bytes
        metadata: bytes

    def __init__(self, database: str):
        super().__init__()
        self._database = database
        self._connection: aiosqlite.Connection = None
        self._subscriptions: List[AIOSQLiteEventStoreSubscription] = list()

    async def _get_last_commit_position(self) -> int | None:
        sql = '''SELECT MAX(commit_position) AS last_commit_position FROM events'''
        async with self._connection.cursor() as cursor:
            await cursor.execute(sql)
            row = await cursor.fetchone()
            return row[0]

    async def _initialize_database(self) -> None:
        logger.info(f'Initializing {self.__class__.__qualname__}')
        create_sql = '''
        CREATE TABLE IF NOT EXISTS events (
            commit_position INT PRIMARY KEY NOT NULL,
            stream_name VARCHAR NOT NULL,
            stream_position INT NOT NULL,
            event_type VARCHAR NOT NULL,
            payload BLOB NOT NULL,
            metadata BLOB NOT NULL,
            unique(stream_name, stream_position)
        )
        '''
        index_sql = 'CREATE INDEX IF NOT EXISTS events_event_type_index ON events (event_type)'

        async with self._connection.cursor() as cursor:
            await cursor.execute(create_sql)
            await cursor.execute(index_sql)
            await self._connection.commit()
        logger.info(f'{self.__class__.__qualname__} initialized')

    async def append_events(self, stream_name: str, events: Tuple[NewStreamEvent]) -> None:
        if not events:
            return
        if not all([isinstance(e, NewStreamEvent) for e in events]):
            raise TypeError(f'Failed to append events into {self.__class__.__qualname__}, '
                            f'all events must be instances of NewStreamEvent class, '
                            f'one or more events are of invalid type, please check types of the passed events.')

        logger.debug(f'Appending {len(events)} into {self.__class__.__qualname__}')


        stream_version = await self.get_stream_version(stream_name)

        new_events: List[AIOSQLiteEventStore.NewEvent] = list()

        commit_position = await self._get_last_commit_position()
        if commit_position is None:
            commit_position = 0
        else:
            commit_position = commit_position + 1

        stream_position = stream_version + 1
        for event in events:
            new_event = self.NewEvent(
                commit_position=commit_position,
                stream_name=stream_name,
                stream_position=stream_position,
                event_type=event.event_type,
                payload=event.payload,
                metadata=event.metadata
            )
            new_events.append(new_event)
            commit_position += 1
            stream_position += 1

        sql = '''
        INSERT INTO events (commit_position, stream_name, stream_position, event_type, payload, metadata)
        VALUES (:commit_position, :stream_name, :stream_position, :event_type, :payload, :metadata)
        '''

        try:
            async with self._connection.cursor() as cursor:
                for new_event in new_events:
                    params = {
                        'commit_position': new_event.commit_position,
                        'stream_name': new_event.stream_name,
                        'stream_position': new_event.stream_position,
                        'event_type': new_event.event_type,
                        'payload': new_event.payload,
                        'metadata': new_event.metadata
                    }
                    await cursor.execute(sql, parameters=params)
            await self._connection.commit()
            logger.debug(f'New events appended to {self.__class__.__qualname__}')
        except Exception as e:
            await self._connection.rollback()
            logger.exception(f'An exception occured when appending new events: {e}')
            raise e

    async def connect(self) -> None:
        logger.info(f'Connecting to {self.__class__.__qualname__}')
        if self._connection is not None:
            logger.info(f'Already connected to {self.__class__.__qualname__}')
            return

        self._connection = await aiosqlite.connect(self._database)
        await self._initialize_database()
        logger.info(f'Connected to {self.__class__.__qualname__}')

    async def close(self) -> None:
        logger.info(f'Closing {self.__class__.__qualname__}')
        for subscription in self.get_subscriptions():
            await subscription.stop()
        if self._connection is not None:
            await self._connection.close()
        logger.info(f'{self.__class__.__qualname__} closed')

    async def create_subscription(self, subscription_name: str, subscription_parameters: 'AIOSQLiteSubscriptionParameters') -> 'AIOSQLiteEventStoreSubscription':
        # TODO - logs
        subscription = AIOSQLiteEventStoreSubscription(
            name=subscription_name,
            parameters=subscription_parameters,
            connection=self._connection
        )
        await subscription.start()
        self._subscriptions.append(subscription)
        return subscription

    async def create_subscription_to_all(self, subscription_name: str, start_position: int) -> 'AIOSQLiteEventStoreSubscription':
        parameters = AIOSQLiteSubscriptionParameters(commit_position=start_position)
        subscription = await self.create_subscription(subscription_name=subscription_name, subscription_parameters=parameters)
        return subscription

    async def create_subscription_to_events(self, subscription_name: str, event_types: List[str],
                                            start_position: int) -> 'AIOSQLiteEventStoreSubscription':
        parameters = AIOSQLiteSubscriptionParameters(
            commit_position=start_position,
            event_types=event_types
        )
        subscription = await self.create_subscription(
            subscription_name=subscription_name,
            subscription_parameters=parameters
        )
        return subscription

    async def create_subscription_to_stream(self, subscription_name: str, stream_name: str, start_position: int) -> 'AIOSQLiteEventStoreSubscription':
        parameters = AIOSQLiteSubscriptionParameters(
            commit_position=start_position,
            stream_names=[stream_name]
        )
        subscription = await self.create_subscription(subscription_name=subscription_name, subscription_parameters=parameters)
        return subscription

    async def get_stream(self, stream_name: str) -> Tuple[StreamEvent]:
        sql = '''
        SELECT *
        FROM events
        WHERE stream_name = :stream_name
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

    async def get_stream_version(self, stream_name: str) -> int:
        # TODO - logs

        sql = '''
        SELECT COALESCE(MAX(stream_position), -1) AS stream_version
        FROM events
        WHERE stream_name = :stream_name
        '''

        async with self._connection.cursor() as cursor:
            params = {'stream_name': stream_name}
            await cursor.execute(sql, parameters=params)
            result = await cursor.fetchone()

        stream_version = result[0]
        return stream_version

    def get_subscriptions(self) -> Tuple[AIOSQLiteEventStoreSubscription, ...]:
        return tuple(self._subscriptions)

    async def stream_exists(self, stream_name: str) -> bool:
        # TODO - logs
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

    def __init__(self, database: str, checkpoint_store: CheckpointStore):
        super().__init__(checkpoint_store=checkpoint_store)
        self._database = database
        self._connection: aiosqlite.Connection = None

    @property
    def connection(self) -> aiosqlite.Connection:
        return self._connection

    @property
    def database_name(self) -> str:
        return self.get_database_name()

    async def drop(self) -> None:
        sql = f'DELETE FROM {self.database_name}'
        cursor = await self.connection.cursor()
        await cursor.execute(sql)
        await cursor.close()
        await self.connection.commit()

    @abstractmethod
    def get_database_name(self) -> str:
        raise NotImplementedError

    async def initialize_connection(self) -> None:
        self._connection = await aiosqlite.connect(self._database)

    async def stop(self) -> None:
        await super().stop()
        await self.connection.close()
