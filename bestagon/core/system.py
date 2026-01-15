import asyncio
import logging
from abc import ABC
from typing import List, Any, Tuple

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_processor import Application, Projection, EventProcessor
from bestagon.core.event_store import EventStore, ApplicationSubscription
from bestagon.core.mapper import mapper
from bestagon.core.message import Command, Query


logger = logging.getLogger(__name__)


class EventSourcedSystem(ABC):
    # TODO - application graph
    # TODO - split workflow into two parts - application workflow (write side) and projection workflow (read side)

    def __init__(self, event_store: EventStore, checkpoint_store: CheckpointStore):
        self._event_store = event_store
        self._checkpoint_store = checkpoint_store
        self._applications: List[Application] = list()
        self._projections: List[Projection] = list()
        self._subscription_tasks: List[asyncio.Task] = list()

    @property
    def applications(self) -> Tuple[Application, ...]:
        return tuple(self._applications)

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def event_store(self) -> EventStore:
        return self._event_store

    @property
    def projections(self) -> Tuple[Projection, ...]:
        return tuple(self._projections)

    async def _add_event_processor(self, event_processor: EventProcessor) -> None:
        if isinstance(event_processor, Projection):
            await event_processor.initialize()

        checkpoint_name = f'{event_processor.name}_subscription'
        checkpoint = await self.checkpoint_store.get_checkpoint(name=checkpoint_name)
        es_subscription = await self.event_store.create_subscription_to_all(subscription_id=checkpoint_name, start_position=checkpoint)
        app_subscription = ApplicationSubscription(subscription_id=es_subscription.subscription_id, event_store_subscription=es_subscription)
        task = event_processor.set_subscription(app_subscription)
        task.add_done_callback(self._when_subscription_task_done)
        self._subscription_tasks.append(task)
        logger.info(f'Event processor {event_processor.name} with subscription {checkpoint_name} added')

    def _when_subscription_task_done(self, task: asyncio.Task) -> None:
        try:
            result = task.result()
            logger.info(f'Task {task.get_name()} finished with result: {result}')
        except StopAsyncIteration:
            logger.info(f'Task {task.get_name()} finished: subscription stop')
        except Exception as e:
            asyncio.create_task(self.shutdown())
            raise e

    async def add_application(self, app: Application) -> None:
        await self._add_event_processor(event_processor=app)
        self._applications.append(app)

    async def add_projection(self, proj: Projection) -> None:
        await self._add_event_processor(event_processor=proj)
        self._projections.append(proj)

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
        await self.event_store.connect()
        await self.checkpoint_store.initialize()

    async def shutdown(self) -> None:
        for app in self.applications:
            await app.stop()

        for proj in self.projections:
            await proj.stop()

        for task in self._subscription_tasks:
            task.cancel()

        await self.event_store.close()
        await self.checkpoint_store.close()