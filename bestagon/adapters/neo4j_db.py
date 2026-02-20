import asyncio
import logging
from abc import abstractmethod
from typing import Tuple

import neo4j

from bestagon.core.checkpoint_store import CheckpointStore, Checkpoint
from bestagon.core.event_processor import Projection

logger = logging.getLogger(__name__)


class Neo4jCheckpointStore(CheckpointStore):
    def __init__(self, driver: neo4j.AsyncDriver, database_name: str):
        self.driver = driver
        self.database_name = database_name

    async def close(self) -> None:
        pass

    async def delete_checkpoint(self, name: str) -> None:
        cypher = '''
        MATCH (c:Checkpoint {name: $name})
        DETACH DELETE c
        '''
        async with self.driver.session(default_access_mode=neo4j.WRITE_ACCESS, database=self.database_name) as sess:
            await sess.run(cypher, name=name)
        logger.info(f'Checkpoint {name} deleted')

    async def get_checkpoint(self, name: str) -> Checkpoint:
        cypher = '''
        MATCH (c:Checkpoint {name: $name})
        RETURN c.value
        '''
        async with self.driver.session(database=self.database_name, default_access_mode=neo4j.READ_ACCESS) as sess:
            result = await sess.run(
                cypher,  # NOQA
                name=name
            )
            value = await result.single()
            if value is not None:
                value = value.value()
            else:
                value = 0

        checkpoint = Checkpoint(name=name, value=value)
        return checkpoint

    async def initialize(self) -> None:
        create_cypher = "CREATE DATABASE $database_name IF NOT EXISTS"
        async with self.driver.session() as sess:
            await sess.run(
                create_cypher,  # NOQA
                database_name=self.database_name
            )

        index_cypher = '''
        CREATE RANGE INDEX checkpoint_name_index
        IF NOT EXISTS
        FOR (n:Checkpoint)
        ON n.name
        '''
        async with self.driver.session(database=self.database_name) as sess:
            await sess.run(index_cypher)  # NOQA

    async def list_checkpoints(self) -> Tuple[Checkpoint, ...]:
        cypher = '''
        MATCH (c:Checkpoint)
        WITH {
            name: c.name,
            value: c.value
        } AS data
        RETURN data
        '''
        async with self.driver.session(default_access_mode=neo4j.READ_ACCESS, database=self.database_name) as sess:
            result = await sess.run(cypher)
            data = await result.value()
        checkpoints = tuple(Checkpoint(**datum) for datum in data)
        return checkpoints

    async def set_checkpoint(self, checkpoint: Checkpoint) -> None:
        cypher = '''
        MERGE (c:Checkpoint {name: $name})
        SET c.value = $value
        '''

        async with self.driver.session(database=self.database_name, default_access_mode=neo4j.WRITE_ACCESS) as sess:
            await sess.run(
                cypher,  # NOQA
                name=checkpoint.name,
                value=checkpoint.value
            )
        logger.debug(f'New checkpoint set for {checkpoint.name}: {checkpoint.value}')


class Neo4jProjection(Projection):
    def __init__(self, driver: neo4j.AsyncDriver, checkpoint_store: CheckpointStore):
        super().__init__(checkpoint_store=checkpoint_store)
        self.driver = driver

    @property
    def database_name(self) -> str:
        return self.get_database_name()

    async def drop(self) -> None:
        # TODO - indexes
        logger.info(f'Dropping projection {self.name}...')
        cypher = '''CREATE OR REPLACE DATABASE $database_name'''
        check_cypher = '''
        SHOW DATABASES
        YIELD name, currentStatus
        WHERE name = $database_name
        RETURN currentStatus AS status
        '''
        async with self.driver.session() as sess:
            await sess.run(cypher, database_name=self.database_name)

            logger.info('Checking database created')
            while True:
                result = await sess.run(check_cypher, database_name=self.database_name)
                record = await result.single()
                status = record.value()
                print(status)
                if status != 'online':
                    logger.info('Waiting for database')
                    await asyncio.sleep(1)
                else:
                    logger.info(f'Database created')
                    break

    @abstractmethod
    def get_database_name(self) -> str:
        raise NotImplementedError

    async def initialize(self) -> None:
        # TODO - indexes
        cypher = '''CREATE DATABASE $database_name IF NOT EXISTS'''
        async with self.driver.session() as sess:
            await sess.run(cypher, database_name=self.database_name)
