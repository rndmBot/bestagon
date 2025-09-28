from dataclasses import dataclass, asdict
from datetime import datetime, timezone


"""
NOTE - domain events  should not contain value objects, only primitives that can be easily serializable.
Value objects contain business logic and validation logic, but events are just facts that have happened.
Events are immutble, value objects are mutable, the change in value object's logic can lead to unseen consequences in aggregate's lifecycle.

Also, using primitimives makes the system less complex by eliminating unnecessary adapters like transcoders and transcodings.
"""


@dataclass(frozen=True)
class DomainEvent:
    # TODO - ACHTUNG - design of event should be reconsidered - must include metadata (correlation_id, causation_id, event_id, ...)
    timestamp: str
    aggregate_id: str
    aggregate_version: int

    @staticmethod
    def create_timestamp() -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    def get_metadata(self) -> dict:
        metadata = dict()
        metadata['timestamp'] = self.timestamp
        metadata['aggregate_id'] = self.aggregate_id
        metadata['aggregate_version'] = self.aggregate_version
        return metadata

    def get_payload(self) -> dict:
        payload = asdict(self).copy()
        payload.pop('timestamp')
        payload.pop('aggregate_id')
        payload.pop('aggregate_version')
        return payload


@dataclass(frozen=True)
class Created(DomainEvent):
    """Legacy"""
    pass
