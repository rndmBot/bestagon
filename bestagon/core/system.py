import logging
from abc import ABC, abstractmethod
from typing import List

from bestagon.core.event_processor import Application, Projection
from bestagon.core.message_bus import AsyncCommandBus, AsyncQueryBus


logger = logging.getLogger(__name__)


class EventSourcedSystem(ABC):
    # TODO - application graph
    # TODO - split workflow into two parts - application workflow (write side) and projection workflow (read side)

    # TODO - how to automatically register command handlers???
    # TODO - how to automatically register query handlers???

    # TODO - hide comand bus and query bus under 'execute_command' and 'execute_query' methods

    def __init__(self):
        self._command_bus = AsyncCommandBus()
        self._query_bus = AsyncQueryBus()
        self._applications: List[Application] = list()  # TODO - add mechaism to add applications
        self._projections: List[Projection] = list()  # TODO - add mechanism to add projections

    @property
    def applications(self) -> List[Application]:
        return self._applications

    @property
    def command_bus(self) -> AsyncCommandBus:
        return self._command_bus

    @property
    def projections(self) -> List[Projection]:
        return self._projections

    @property
    def query_bus(self) -> AsyncQueryBus:
        return self._query_bus

    # TODO - concept
    # async def add_event_processor(self, event_processor: EventProcessor) -> None:
    #     if isinstance(event_processor, Projection):
    #         await event_processor.initialize()
    #
    #     checkpoint_name = f'{event_processor.name}_subscription'  # TODO - ORLY???
    #     checkpoint = await self.checkpoint_store.get_checkpoint(name=checkpoint_name)
    #     # TODO - ACHTUNG - currently subscribes to all, restrict subscriptions only to required events to improve performancej
    #     es_subscription = await self.event_store.create_subscription_to_all(subscription_id=checkpoint_name, start_position=checkpoint)
    #     de_subscription = AsyncDomainEventsSubscription(subscription_id=es_subscription.subscription_id, event_store_subscription=es_subscription)
    #
    #     task = event_processor.set_subscription(de_subscription)
    #     task.add_done_callback(self._when_subscription_task_done)
    #     self.subscription_tasks.append(task)
    #     logger.info(f'Event processor {event_processor.name} with subscription {checkpoint_name} added')

    # TODO - concept
    # def _when_subscription_task_done(self, task: asyncio.Task) -> None:
    #     try:
    #         result = task.result()
    #         logger.info(f'Task {task.get_name()} finished with result: {result}')
    #     except StopAsyncIteration:
    #         logger.info(f'Task {task.get_name()} finished: subscription stop')
    #     except Exception as e:
    #         asyncio.create_task(self.shutdown())
    #         raise e

    @abstractmethod
    def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        raise NotImplementedError
