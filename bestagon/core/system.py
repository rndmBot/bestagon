import datetime
import logging
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Any, Tuple, Dict
from uuid import uuid4

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_processor import Application, Projection
from bestagon.core.event_store import EventStore
from bestagon.core.message import Command, Query, TracingMetadata, CommandMetadata, CommandMessage
from bestagon.core.registry import command_handler_registry, query_handler_registry

logger = logging.getLogger(__name__)


class SystemStatus(Enum):
    ONLINE = 'online'
    WARNING = 'warning'


@dataclass(frozen=True)
class SystemHealth:
    status: SystemStatus
    application_status: Dict[str, bool]
    projection_status: Dict[str, bool]


class EventSourcedSystem(ABC):
    # TODO - IDEA - make add_application/projection synchronous and add apps to queue, on initialization it should be consumed and apps should be initialized

    def __init__(self, event_store: EventStore, checkpoint_store: CheckpointStore):
        self._event_store = event_store
        self._checkpoint_store = checkpoint_store
        self._applications: Dict[str, Application] = dict()
        self._projections: Dict[str, Projection] = dict()

    @property
    def applications(self) -> Tuple[Application, ...]:
        apps = tuple(self._applications.values())
        return apps

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def projections(self) -> Tuple[Projection, ...]:
        projs = tuple(self._projections.values())
        return projs

    async def add_application(self, app: Application) -> None:
        if app.name in self._applications:
            raise ValueError(f'Application with name {app.name} have already been added to the system')
        await app.subscribe_to(self.event_store)
        self._applications[app.name] = app
        # TODO - the system should stop if there is a failure in any application

    async def add_projection(self, proj: Projection) -> None:
        if proj.name in self._projections:
            raise ValueError(f'Projection with name {proj.name} have already been added to the system')
        await proj.initialize()
        await proj.subscribe_to(self.event_store)
        self._projections[proj.name] = proj

    @staticmethod
    async def execute_command(command: Command, tracing_metadata: TracingMetadata | None) -> None:
        # TODO - there should be another component responsible for metadata creation and propagation - MetadataFactory
        command_id = str(uuid4())

        if tracing_metadata is None:
            tracing_metadata = TracingMetadata(
                correlation_id=str(uuid4()),
                causation_id=command_id
            )

        command_metadata = CommandMetadata(
            timestamp=datetime.datetime.now(datetime.UTC).isoformat(),
            command_id=command_id,
            command_type=command.__class__.__qualname__,
            correlation_id=tracing_metadata.correlation_id,
            causation_id=tracing_metadata.causation_id
        )
        command_message = CommandMessage(
            command=command,
            metadata=command_metadata
        )

        command_handler = command_handler_registry.get_command_handler(command_type=type(command))
        await command_handler(command_message)

    @staticmethod
    async def execute_query(query: Query) -> Any:
        # TODO - FUTURE - there should be query bus
        # TODO - query bus can provide query interceptors for validation and query modification
        query_handler = query_handler_registry.get_query_handler(type(query))
        result = await query_handler(query)
        return result

    def get_health(self) -> SystemHealth:
        running_apps = sum([app.running for app in self.applications])
        running_projs = sum([proj.running for proj in self.projections])

        if running_apps != len(self.applications):
            status = SystemStatus.WARNING
        elif running_projs != len(self.projections):
            status = SystemStatus.WARNING
        else:
            status = SystemStatus.ONLINE

        health = SystemHealth(
            status=status,
            application_status={app.name: app.running for app in self.applications},
            projection_status={proj.name: proj.running for proj in self.projections}
        )
        return health

    def get_projection(self, name: str) -> Projection:
        projection = self._projections.get(name)
        if projection is None:
            raise ValueError(f'No projection with name {name} found')
        return projection

    async def initialize(self) -> None:
        self.register_command_handlers()
        self.register_query_handlers()
        self.register_aggregate_types()
        self.register_event_types()
        await self.event_store.initialize()
        await self.checkpoint_store.initialize()

    async def rebuild_projection(self, name: str) -> None:
        projection = self.get_projection(name)
        checkpoint_name = projection.get_checkpoint_name()
        await projection.stop()
        await projection.drop()
        await projection.checkpoint_store.delete_checkpoint(checkpoint_name)
        await projection.subscribe_to(self.event_store)
        logger.info(f'Projection {name} rebuild started')

    def register_aggregate_types(self) -> None:
        pass

    def register_command_handlers(self) -> None:
        pass

    def register_event_types(self) -> None:
        pass

    def register_query_handlers(self) -> None:
        pass

    async def shutdown(self) -> None:
        for app in self.applications:
            await app.stop()

        for proj in self.projections:
            await proj.stop()

        await self.event_store.shutdown()
        await self.checkpoint_store.close()