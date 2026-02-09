from dataclasses import dataclass, asdict
from datetime import datetime, timezone


@dataclass(frozen=True)
class Message:
    pass


@dataclass(frozen=True)
class Command(Message):
    # TODO - commands sent through a command bus should allow the result to be returned
    pass


@dataclass(frozen=True)
class Query(Message):
    pass


@dataclass(frozen=True)
class DomainEventMetadata:
    """
    DO NOT base business decisions on metadata
    The correlation_id of a message always references the identifier of the message it originates from (that is, the parent message).
    The trace_id on the other hand references to the message identifier which started the chain of messages (that is, the root message).
    """
    # TODO - add correlation_id
    # TODO - add trace_id
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
    # TODO - inherit from Message
    metadata: DomainEventMetadata

    def get_metadata_as_dict(self) -> dict:
        return asdict(self.metadata)

    def get_payload(self) -> dict:
        payload = asdict(self).copy()
        payload.pop('metadata')
        return payload


@dataclass(frozen=True)
class Created(DomainEvent):
    pass


@dataclass(frozen=True)
class ApplicationEvent:
    # TODO - ORLY - inherit from Message???
    commit_position: int
    domain_event: DomainEvent


# TODO - belong to event store module
@dataclass(frozen=True)
class NewStreamEvent:
    """New event to store in event store"""
    stream_position: int  # Position in aggreate sequence
    event_type: str
    payload: bytes
    metadata: bytes


# TODO - belong to event store module
@dataclass(frozen=True)
class StreamEvent:
    """Event retreived from EventStore"""
    stream_name: str
    stream_position: int  # Position in aggreate sequence
    commit_position: int  # Position in event store sequence
    event_type: str
    payload: bytes
    metadata: bytes
