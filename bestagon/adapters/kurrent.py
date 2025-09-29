from typing import List

from kurrentdbclient import KurrentDBClient, StreamState, NewEvent

from bestagon.adapters.event_store import EventStore
from bestagon.adapters.stored_event import StoredEvent
from bestagon.exceptions import IntegrityError


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

    def get_events(self, regex_list: str, start_position: int, limit: int) -> List[StoredEvent]:
        recorded_events = self.client.read_all(
            commit_position=start_position,
            filter_include=regex_list,
            filter_by_stream_name=True,
            limit=limit
        )

        events = list()
        for recorded_event in recorded_events:
            stored_event = StoredEvent(
                stream_name=recorded_event.stream_name,
                stream_position=recorded_event.stream_position,
                commit_position=recorded_event.commit_position,
                event_type=recorded_event.type,
                payload=recorded_event.data,
                metadata=recorded_event.metadata
            )
            events.append(stored_event)
        return events

    def get_stream(self, stream_name: str) -> List[StoredEvent]:
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
