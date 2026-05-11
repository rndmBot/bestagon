import logging
from abc import abstractmethod
from typing import Tuple

import aiosqlite
from bestagon.core.checkpoint_store import CheckpointStore, Checkpoint
from bestagon.core.event_processor import Projection

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
