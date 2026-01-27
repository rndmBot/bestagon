import logging
from abc import ABC
from typing import Any, Tuple, Dict

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_processor import Application, Projection, EventProcessor
from bestagon.core.event_store import EventStore, ApplicationSubscription
from bestagon.core.mapper import mapper
from bestagon.core.message import Command, Query


logger = logging.getLogger(__name__)


class EventSourcedSystem(ABC):
    # TODO - application graph
    # TODO - split workflow into two parts - application workflow (write side) and projection workflow (read side)
    # TODO - add possibility to rebuild projection

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

    async def _subscribe_event_processor(self, event_processor: EventProcessor) -> None:
        if isinstance(event_processor, Projection):
            await event_processor.initialize()

        checkpoint_name = f'{event_processor.name}_subscription'
        checkpoint = await self.checkpoint_store.get_checkpoint(name=checkpoint_name)
        es_subscription = await self.event_store.create_subscription_to_all(subscription_id=checkpoint_name, start_position=checkpoint)
        app_subscription = ApplicationSubscription(subscription_id=es_subscription.subscription_id, event_store_subscription=es_subscription)
        event_processor.set_subscription(app_subscription)
        logger.info(f'Event processor {event_processor.name} with subscription {checkpoint_name} added')

    async def add_application(self, app: Application) -> None:
        if app.name in self._applications:
            raise ValueError(f'Application with name {app.name} have already been added to the system')
        await self._subscribe_event_processor(event_processor=app)
        self._applications[app.name] = app

    async def add_projection(self, proj: Projection) -> None:
        if proj.name in self._projections:
            raise ValueError(f'Projection with name {proj.name} have already been added to the system')
        await self._subscribe_event_processor(event_processor=proj)
        self._projections[proj.name] = proj

    @staticmethod
    async def execute_command(command: Command) -> None:
        """
        TODO - command bus
            There should be interceptors on command bus, they can alter the command message by adding metadata. They can also block the command by throwing an exception.

        TODO - Structural validation
            There is no point in processing a command if it does not contain all required information in the correct format.
            In fact, a command that lacks information should be blocked as early as possible, preferably even before a transaction has been started.
            Therefore, an interceptor should check all incoming commands for the availability of such information. This is called structural validation.
        """
        # TODO - FUTURE - there should be command bus
        command_handler = mapper.get_command_handler(command_type=type(command))
        await command_handler(command)

    @staticmethod
    async def execute_query(query: Query) -> Any:
        # TODO - FUTURE - there should be query bus
        # TODO - query bus can provide query interceptors for validation and query modification
        query_handler = mapper.get_query_handler(type(query))
        result = await query_handler(query)
        return result

    async def initialize(self) -> None:
        self.register_command_handlers()
        self.register_query_handlers()
        self.register_aggregate_types()
        self.register_event_types()
        await self.event_store.connect()
        await self.checkpoint_store.initialize()

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

        await self.event_store.close()
        await self.checkpoint_store.close()