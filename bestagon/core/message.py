import datetime
from dataclasses import dataclass, asdict
from uuid import uuid4


@dataclass(frozen=True)
class DomainEvent:
    """
    Domain event represents an important change in a business domain that is meaningfull to business experts and stakeholders.
    Domain event leads to the change in aggregate state and at the same time triggers a reaction in other parts of the system.
    """
    pass


@dataclass
class DomainEventMetadata:
    """
    Key domain event metadata. Contains all technical attributes of the event.

    :param event_id: Unique identifier for this specific event instance
    :param timestamp: ISO 8601 timestamp when the event was created
    :param aggregate_id: ID of the aggregate the event belongs to
    :param aggregate_version: version of the aggregate after creation of the event
    :param aggregate_type: type of the aggregate event belongs to

    :param correlation_id: a unique identifier attached to a request that remains consistent as the request passes through multiple services.
    :param causation_id: ID of the event that triggered this one
    """
    timestamp: str
    event_id: str
    event_type: str
    aggregate_id: str
    aggregate_version: int
    aggregate_type: str

    # Tracing identifiers
    correlation_id: str
    causation_id: str

    @staticmethod
    def create_event_id() -> str:
        return str(uuid4())

    @staticmethod
    def create_timestamp() -> str:
        return datetime.datetime.now(datetime.UTC).isoformat()


    @classmethod
    def from_dict(cls, data: dict) -> 'DomainEventMetadata':
        obj = cls(**data)
        return obj

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Command:
    """
    A fundamental concept of event sourcing that is easily overlooked: commands are inextricably linked to the state of the world at the time the command was created.
    Even more plainly: never enqueue commands. The instant that command is enqueued it becomes irrelevant because the state of the world has moved on.
    Instead of enqueuing them, commands should be be processed via request/reply. A live service should handle the command request, validate it as of the current state,
    and reject it accordingly or return a list of events. Not only does this give our application the chance to get more robust error messages as to why a command was rejected,
    but it also ensures that bad commands can't produce events. There will never exist an event produced from a stale command.

    Source: https://blog.cosmonic.com/engineering/commands-are-not-real/
    """
    pass


@dataclass(frozen=True)
class CommandMetadata:
    timestamp: str
    command_id: str
    command_type: str

    # Tracing identifiers
    correlation_id: str
    causation_id: str


@dataclass(frozen=True)
class Query:
    # TODO - there is streaming query and subscription query
    pass


@dataclass(frozen=True)
class Message:
    pass


@dataclass(frozen=True)
class DomainMessage(Message):
    event: DomainEvent
    metadata: DomainEventMetadata


@dataclass(frozen=True)
class CommandMessage(Message):
    command: Command
    metadata: CommandMetadata