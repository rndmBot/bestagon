import json

from bestagon.core.event_store import EventStoreEvent, NewEventStoreEvent
from bestagon.core.message import DomainMessage, DomainEventMetadata
from bestagon.core.registry import event_type_registry


# TODO - mapper module is deprecated, these functions should be moved to other part of framework


def to_domain_message(event_store_event: EventStoreEvent) -> DomainMessage:
    event_class = event_type_registry.get_event_class(event_store_event.event_type)
    payload_dict = json.loads(event_store_event.payload.decode())
    domain_event = event_class(**payload_dict)

    metadata_dict = json.loads(event_store_event.metadata.decode())
    metadata = DomainEventMetadata.from_dict(metadata_dict)  # TODO - no backward compatibility mechanism, should be implemented in future versions

    message = DomainMessage(
        event=domain_event,
        metadata=metadata
    )
    return message


def to_new_event_store_event(domain_message: DomainMessage) -> NewEventStoreEvent:
    payload = json.dumps(domain_message.event.to_dict()).encode()
    metadata = json.dumps(domain_message.metadata.to_dict()).encode()

    new_stream_event = NewEventStoreEvent(
        event_type=domain_message.metadata.event_type,
        payload=payload,
        metadata=metadata
    )
    return new_stream_event
