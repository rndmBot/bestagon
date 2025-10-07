import neo4j

from bestagon.core.checkpoint_store import CheckpointStore


class Neo4jCheckpointStore(CheckpointStore):
    def __init__(self, driver: neo4j.Driver, database_name: str):
        self.driver = driver
        self.database_name = database_name
        self.create_database()

    def create_database(self) -> None:
        create_cypher = "CREATE DATABASE $database_name IF NOT EXISTS"
        with self.driver.session() as sess:
            sess.run(
                create_cypher,  # NOQA
                database_name=self.database_name
            )

        index_cypher = '''
        CREATE RANGE INDEX checkpoint_name_index
        IF NOT EXISTS
        FOR (n:Checkpoint)
        ON n.name
        '''

        with self.driver.session(database=self.database_name) as sess:
            sess.run(index_cypher)  # NOQA

    def get_checkpoint(self, name: str) -> int:
        cypher = '''
        MATCH (c:Checkpoint {name: $name})
        RETURN c.value
        '''

        with self.driver.session(database=self.database_name, default_access_mode=neo4j.READ_ACCESS) as sess:
            result = sess.run(
                cypher,  # NOQA
                name=name
            )
            data = result.single()
            if data is not None:
                data = data.value()
            else:
                data = 0

        return data

    def set_checkpoint(self, name: str, value: int) -> None:
        cypher = '''
        MERGE (c:Checkpoint {name: $name})
        SET c.value = $value
        '''

        with self.driver.session(database=self.database_name, default_access_mode=neo4j.WRITE_ACCESS) as sess:
            sess.run(
                cypher,  # NOQA
                name=name,
                value=value
            )
