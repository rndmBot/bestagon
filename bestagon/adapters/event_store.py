import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from itertools import pairwise
from typing import List, Dict, Optional

from bestagon.exceptions import IntegrityError, InvalidPositionError


@dataclass(frozen=True)
class StoredEvent:
    """Event to be saved in and retreived from EventStore"""
    stream_name: str
    stream_position: int  # Position in aggreate sequence
    commit_position: Optional[int]  # Position in application sequence
    event_type: str
    payload: bytes
    metadata: bytes

    def __eq__(self, other):
        if isinstance(other, StoredEvent):
            eq = all(
                [
                    self.stream_name == other.stream_name,
                    self.stream_position == other.stream_position
                ]
            )
            return eq
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, StoredEvent):
            return self.stream_position < other.stream_position
        return NotImplemented


class EventStore(ABC):
    def __contains__(self, item):
        return self.stream_exists(stream_name=item)

    @staticmethod
    def _check_events_gapless(events: List[StoredEvent]) -> None:
        """
        Events should have monotonicaly increasing stream position, ex: 0, 1, 2, 3
        Any gaps or out of order events are not allowed.
        """
        versions = [event.stream_position for event in events]
        diffs = [y - x for x, y in pairwise(versions)]
        gapless = all([True if d == 1 else False for d in diffs])
        if not gapless:
            raise IntegrityError('Events must be gapless to record in event store.')

    @staticmethod
    def _check_events_homogeneous(events: List[StoredEvent]) -> None:
        """Events from multiple streams are not allowed."""
        ids = set()
        for event in events:
            ids.add(event.stream_name)
            if len(ids) > 1:
                raise IntegrityError('Record to multiple streams are not supported.')

    @staticmethod
    def _check_events_unique(events: List[StoredEvent]) -> None:
        """Events must be unique to be recorded in event store."""
        if len(set(events)) < len(events):
            raise IntegrityError('Events must be unique to record to event store.')

    @abstractmethod
    def append_events(self, stream_name: str, events: List[StoredEvent]) -> None:
        """
        Adds new events to specified event stream.
        ACHTUNG - validate events before recording them into the event store.
        """
        raise NotImplementedError

    def validate_events(self, events: List[StoredEvent]) -> None:
        """Convenience class."""
        self._check_events_unique(events)
        self._check_events_gapless(events)
        self._check_events_homogeneous(events)

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_events(self, regex_list: List[str], start_position: int, limit: int) -> List[StoredEvent]:
        """Allows to retreive events from specific streams specified by regex, strating from specific position."""
        raise NotImplementedError

    @abstractmethod
    def get_stream(self, stream_name: str) -> List[StoredEvent]:
        """Returns all events from a single stream."""
        raise NotImplementedError

    @abstractmethod
    def stream_exists(self, stream_name: str) -> bool:
        """Returns True if specified stream exists in event store."""
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

    def get_stream(self, stream_name: str) -> List[StoredEvent]:
        stream_data = self._streams.get(stream_name)
        if stream_data is None:
            return list()

        indexes = sorted(stream_data.values())
        events = [self._events[index] for index in indexes]
        return events

    def get_events(self, regex_list: List[str], start_position: int, limit: int) -> List[StoredEvent]:
        if not regex_list:
            indexes = range(len(self._events))
        else:
            streams = list()
            indexes = list()

            for stream_name in self._streams:
                for regex in regex_list:
                    if re.match(regex, stream_name):
                        streams.append(stream_name)
                        break

            for stream_name in streams:
                indexes.extend(list(self._streams[stream_name].values()))
            indexes.sort()

        if start_position not in indexes:
            raise InvalidPositionError(f'Invalid start position: {start_position}')

        events = list()
        for i in indexes[start_position:]:
            events.append(self._events[i])
            if len(events) == limit:
                break

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
