import json

from bestagon.adapters.stored_event import DomainEvent, StoredEvent
from bestagon.resolver import resolver


def convert_to_stored_event(domain_event: DomainEvent) -> StoredEvent:
    event_type = resolver.get_topic(domain_event.__class__)
    payload = json.dumps(domain_event.payload).encode('utf8')
    metadata = json.dumps(domain_event.metadata).encode('utf8')

    stored_event = StoredEvent(
        aggregate_id=domain_event.aggregate_id,
        aggregate_version=domain_event.aggregate_version,
        event_type=event_type,
        payload=payload,
        metadata=metadata
    )
    return stored_event


def convert_to_domain_event(stored_event: StoredEvent) -> DomainEvent:
    cls = resolver.resolve_aggregate_event_topic(stored_event.event_type)
    payload = json.loads(stored_event.payload.decode('utf8'))
    metadata = json.loads(stored_event.payload.decode('utf8'))

    obj = cls(
        aggregate_id=stored_event.aggregate_id,
        aggregate_version=stored_event.aggregate_version,
        metadata=metadata,
        **payload
    )
    return obj
