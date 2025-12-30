from dataclasses import dataclass, asdict
from datetime import datetime, timezone


@dataclass(frozen=True)
class Message:
    pass


@dataclass(frozen=True)
class Command(Message):
    pass


@dataclass(frozen=True)
class Query(Message):
    pass


# TODO - IDEA - add ExternalEvent class


@dataclass(frozen=True)
class DomainEventMetadata:
    # TODO - add correlation_id
    # TODO - add causation_id
    # TODO - ORLY - inherit dict and use getters to get aggregate_id, version etc.

    timestamp: str  # TODO - turn it into field to create automatically
    aggregate_id: str
    aggregate_version: int
    aggregate_type: str

    @staticmethod
    def create_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DomainEvent:
    # TODO - each domain event should have its Type, that will be used during convertation to StreamEvent
    # TODO - there should be mechanism to automatically register event type in mapper
    metadata: DomainEventMetadata

    def get_metadata_as_dict(self) -> dict:
        return asdict(self.metadata)

    def get_payload(self) -> dict:
        payload = asdict(self).copy()
        payload.pop('metadata')
        return payload


@dataclass(frozen=True)
class Created(DomainEvent):
    """Legacy"""
    pass


@dataclass(frozen=True)
class ApplicationEvent:
    # TODO - ORLY - inherit from Message???
    commit_position: int
    domain_event: DomainEvent


@dataclass(frozen=True)
class NewStreamEvent:
    """New event to store in event store"""
    stream_position: int  # Position in aggreate sequence
    event_type: str
    payload: bytes
    metadata: bytes


@dataclass(frozen=True)
class StreamEvent:
    """Event retreived from EventStore"""
    stream_name: str
    stream_position: int  # Position in aggreate sequence
    commit_position: int  # Position in event store sequence
    event_type: str
    payload: bytes
    metadata: bytes

    def __eq__(self, other):
        # TODO - ORLY???
        if isinstance(other, StreamEvent):
            eq = all(
                [
                    self.stream_name == other.stream_name,
                    self.stream_position == other.stream_position
                ]
            )
            return eq
        return NotImplemented

    def __lt__(self, other):
        # TODO - ORLY???
        if isinstance(other, StreamEvent):
            return self.stream_position < other.stream_position
        return NotImplemented
