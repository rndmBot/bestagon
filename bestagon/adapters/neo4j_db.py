import logging

import neo4j

from bestagon.core.checkpoint_store import CheckpointStore


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

    async def get_checkpoint(self, name: str) -> int:
        cypher = '''
        MATCH (c:Checkpoint {name: $name})
        RETURN c.value
        '''
        async with self.driver.session(database=self.database_name, default_access_mode=neo4j.READ_ACCESS) as sess:
            result = await sess.run(
                cypher,  # NOQA
                name=name
            )
            data = await result.single()
            if data is not None:
                data = data.value()
            else:
                data = 0

        return data

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

    async def set_checkpoint(self, name: str, value: int) -> None:
        cypher = '''
        MERGE (c:Checkpoint {name: $name})
        SET c.value = $value
        '''

        async with self.driver.session(database=self.database_name, default_access_mode=neo4j.WRITE_ACCESS) as sess:
            await sess.run(
                cypher,  # NOQA
                name=name,
                value=value
            )
        logger.debug(f'New checkpoint set for {name}: {value}')
