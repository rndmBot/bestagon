import re
from collections import defaultdict
from typing import List, Dict

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_store import EventStore, StreamEvent, NewStreamEvent
from bestagon.exceptions import IntegrityError, InvalidPositionError


class POPOCheckpointStore(CheckpointStore):
    def __init__(self):
        self._store = dict()

    def get_checkpoint(self, name: str) -> int:
        return self._store.get(name, 0)

    def set_checkpoint(self, name: str, value: int) -> None:
        self._store[name] = value


class POPOEventStore(EventStore):
    def __init__(self):
        self._events: List[StreamEvent] = list()  # Application sequence
        self._streams: Dict[str, Dict[int, int]] = defaultdict(dict)  # Aggregate sequence {stream_name: {aggregate_version: position_in_application_sequence}}

    def close(self) -> None:
        pass

    def append_events(self, stream_name: str, events: List[NewStreamEvent]) -> None:
        self.validate_new_events(events)

        recorded_versions = self._streams.get(stream_name)
        recorded_versions = recorded_versions or list()

        # There should be no events with already recorded aggregate version
        for event in events:
            if event.stream_position in recorded_versions:
                raise IntegrityError(f'Event with position {event.stream_position} for stream {stream_name} was already recorded.')

        for event in events:
            commit_position = len(self._events)
            event = StreamEvent(
                stream_name=stream_name,
                stream_position=event.stream_position,
                commit_position=commit_position,
                event_type=event.event_type,
                payload=event.payload,
                metadata=event.metadata
            )
            self._events.append(event)
            self._streams[stream_name][event.stream_position] = commit_position

    def get_stream(self, stream_name: str) -> List[StreamEvent]:
        stream_data = self._streams.get(stream_name)
        if stream_data is None:
            return list()

        indexes = sorted(stream_data.values())
        events = [self._events[index] for index in indexes]
        return events

    def get_events(self, regex_list: List[str], start_position: int, limit: int) -> List[StreamEvent]:
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
