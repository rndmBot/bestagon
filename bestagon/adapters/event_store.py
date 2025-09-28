from abc import ABC, abstractmethod
from collections import defaultdict
from itertools import pairwise
from typing import List, Dict

from kurrentdbclient import KurrentDBClient, StreamState, NewEvent

from bestagon.exceptions import IntegrityError
from bestagon.adapters.stored_event import StoredEvent

# TODO - Event Store should provide subscription mechanism


# @dataclass(frozen=True)
# class CheckpointName:
#     leader: str
#     follower: str
#     separator: str = '|->|'
#
#     def __eq__(self, other):
#         if isinstance(other, CheckpointName):
#             return self.name == other.name
#         return NotImplemented
#
#     def __hash__(self):
#         return hash(self.name)
#
#     def __str__(self):
#         return self.name
#
#     @property
#     def name(self) -> str:
#         return f'{self.leader}{self.separator}{self.follower}'


class EventStore(ABC):
    def __contains__(self, item):
        return self.stream_exists(stream_name=item)

    @staticmethod
    def _check_events_gapless(events: List[StoredEvent]) -> None:
        versions = [event.stream_position for event in events]
        diffs = [y - x for x, y in pairwise(versions)]
        gapless = all([True if d == 1 else False for d in diffs])
        if not gapless:
            raise IntegrityError('Events must be gapless to record in event store.')

    @staticmethod
    def _check_events_homogeneous(events: List[StoredEvent]) -> None:
        """Homogeneous events are events from one aggregate_id"""
        ids = set()
        for event in events:
            ids.add(event.stream_name)
            if len(ids) > 1:
                raise IntegrityError('Record to multiple streams are not supported.')

    @staticmethod
    def _check_events_unique(events: List[StoredEvent]) -> None:
        if len(set(events)) < len(events):
            raise IntegrityError('Events must be unique to record to event store.')

    @abstractmethod
    def append_events(self, stream_name: str, events: List[StoredEvent]) -> None:
        raise NotImplementedError

    def validate_events(self, events: List[StoredEvent]) -> None:
        self._check_events_unique(events)
        self._check_events_gapless(events)
        self._check_events_homogeneous(events)

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_events(self, stream_name: str) -> List[StoredEvent]:
        raise NotImplementedError

    @abstractmethod
    def stream_exists(self, stream_name: str) -> bool:
        raise NotImplementedError


# class CheckpointStore(ABC):
#     @abstractmethod
#     def get_checkpoint(self, name: CheckpointName) -> Union[int, None]:
#         raise NotImplementedError
#
#     @abstractmethod
#     def set_checkpoint(self, name: CheckpointName, value: int) -> None:
#         raise NotImplementedError


class POPOEventStore(EventStore):
    def __init__(self):
        self._events: List[StoredEvent] = list()  # Application sequence
        self._streams: Dict[str, Dict[int, int]] = defaultdict(dict)  # Aggregate sequence {stream_name: {aggregate_version: position_in_application_sequence}}

    def close(self) -> None:
        pass

    def append_events(self, stream_name: str, events: List[StoredEvent]) -> None:
        self.validate_events(events)

        recorded_versions = self._streams.get(stream_name)
        recorded_versions = recorded_versions or list()

        # There should be no events with already recorded aggregate version
        for event in events:
            if event.stream_position in recorded_versions:
                raise IntegrityError(f'Event with position {event.stream_position} for stream {stream_name} was already recorded.')

        for event in events:
            commit_position = len(self._events)
            event = StoredEvent(
                stream_name=event.stream_name,
                stream_position=event.stream_position,
                commit_position=commit_position,
                event_type=event.event_type,
                payload=event.payload,
                metadata=event.metadata
            )
            self._events.append(event)
            self._streams[stream_name][event.stream_position] = commit_position

    def get_events(self, stream_name: str) -> List[StoredEvent]:
        stream_data = self._streams.get(stream_name)
        if stream_data is None:
            return list()

        indexes = sorted(stream_data.values())
        events = [self._events[index] for index in indexes]
        return events

    def stream_exists(self, stream_name: str) -> bool:
        return stream_name in self._streams


# class POPOCheckpointStore(CheckpointStore):
#     def __init__(self):
#         self._store = dict()
#
#     def get_checkpoint(self, name: CheckpointName) -> Union[int, None]:
#         return self._store.get(name, None)
#
#     def set_checkpoint(self, name: CheckpointName, value: int) -> None:
#         self._store[name] = value


# class Neo4jCheckpointStore(CheckpointStore):
#     # TODO - ORLY - detach to separate module???
#
#     def __init__(self, driver: neo4j.Driver, database_name: str):
#         self.driver = driver
#         self.database_name = database_name
#         self.create_database()
#
#     def create_database(self) -> None:
#         create_cypher = "CREATE DATABASE $database_name IF NOT EXISTS"
#         with self.driver.session() as sess:
#             sess.run(
#                 create_cypher,  # NOQA
#                 database_name=self.database_name
#             )
#
#         index_cypher = '''
#         CREATE RANGE INDEX checkpoint_name_index
#         IF NOT EXISTS
#         FOR (n:Checkpoint)
#         ON n.name
#         '''
#
#         with self.driver.session(database=self.database_name) as sess:
#             sess.run(index_cypher)  # NOQA
#
#     def get_checkpoint(self, name: CheckpointName) -> Union[int, None]:
#         cypher = '''
#         MATCH (c:Checkpoint {name: $name})
#         RETURN c.value
#         '''
#
#         with self.driver.session(database=self.database_name, default_access_mode=neo4j.READ_ACCESS) as sess:
#             result = sess.run(
#                 cypher,  # NOQA
#                 name=name.name
#             )
#             data = result.single()
#             if data is not None:
#                 data = data.value()
#
#         return data
#
#     def set_checkpoint(self, name: CheckpointName, value: int) -> None:
#         cypher = '''
#         MERGE (c:Checkpoint {name: $name})
#         SET c.value = $value
#         '''
#
#         with self.driver.session(database=self.database_name, default_access_mode=neo4j.WRITE_ACCESS) as sess:
#             sess.run(
#                 cypher,  # NOQA
#                 name=name.name,
#                 value=value
#             )


class KurrentDBEventStore(EventStore):
    def __init__(self, client: KurrentDBClient):
        self.client = client

    def close(self) -> None:
        self.client.close()

    def append_events(self, stream_name: str, events: List[StoredEvent]) -> None:
        self.validate_events(events)

        # Check current version
        current_version = self.client.get_current_version(stream_name=stream_name)
        first_event = events[0]
        if current_version == StreamState.NO_STREAM:
            if first_event.stream_position != 0:
                raise IntegrityError('Failed to append event to non existent stream, events position should start from 0')
        elif first_event.stream_position <= current_version:
            raise IntegrityError(f'Failed to append events - event with version {first_event.stream_position} already exists in database')

        new_events = list()
        for event in events:
            new_event = NewEvent(
                type=event.event_type,
                data=event.payload,
                metadata=event.metadata
            )
            new_events.append(new_event)

        self.client.append_to_stream(
            stream_name=stream_name,
            current_version=current_version,
            events=new_events
        )

    def get_events(self, stream_name: str) -> List[StoredEvent]:
        recorded_events = self.client.get_stream(stream_name=stream_name)
        stored_events = list()

        for recorded_event in recorded_events:
            stored_event = StoredEvent(
                stream_name=recorded_event.stream_name,
                stream_position=recorded_event.stream_position,
                commit_position=recorded_event.commit_position,
                event_type=recorded_event.type,
                payload=recorded_event.data,
                metadata=recorded_event.metadata
            )
            stored_events.append(stored_event)
        return stored_events

    def stream_exists(self, stream_name: str) -> bool:
        current_version = self.client.get_current_version(stream_name=stream_name)
        if current_version == StreamState.NO_STREAM:
            return False
        return True

# def get_application_events(self, application_name: str, start: Union[int, None] = None, limit: int = 10, separator: str = '.') -> List[ApplicationEvent]:
    #     regex = f'{application_name}.*'
    #     recorded_events = self.client.read_all(
    #         filter_include=[regex],
    #         commit_position=start,
    #         filter_by_stream_name=True,
    #         limit=limit
    #     )
    #     stored_events = list()
    #
    #     # TODO - smells
    #     try:
    #         for recorded_event in recorded_events:
    #             aggregate_id = recorded_event.stream_name.replace(f'{application_name}{separator}', '')
    #             stored_event = ApplicationEvent(
    #                 sequence_position=recorded_event.commit_position,
    #                 aggregate_id=aggregate_id,
    #                 aggregate_version=recorded_event.stream_position,
    #                 event_type=recorded_event.type,
    #                 payload=recorded_event.data,
    #                 metadata=recorded_event.metadata
    #             )
    #             stored_events.append(stored_event)
    #     except UnknownError:
    #         # TODO - ACHTUNG - UnknownError can be raised in any other case, not only on invalid position
    #         raise InvalidPositionError(f'Invalid start position {start}.')
    #
    #     return stored_events
