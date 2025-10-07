from dataclasses import dataclass, asdict
from datetime import datetime, timezone


@dataclass(frozen=True)
class DomainEventMetadata:
    # TODO - add correlation_id
    # TODO - add causation_id
    timestamp: str
    aggregate_id: str
    aggregate_version: int
    aggregate_type: str


@dataclass(frozen=True)
class DomainEvent:
    metadata: DomainEventMetadata

    @staticmethod
    def create_timestamp() -> str:
        """Legacy"""
        return datetime.now(tz=timezone.utc).isoformat()

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
