from dataclasses import dataclass, asdict
from datetime import datetime, timezone


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
