import asyncio
import inspect
import logging
from abc import abstractmethod, ABC
from typing import Callable, Type

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.core.event_store import EventStore, ApplicationSubscription
from bestagon.core.mapper import mapper
from bestagon.core.message import DomainEvent, Command
from bestagon.core.repository import AsyncRepository

logger = logging.getLogger(__name__)


class EventProcessor(ABC):
    def __init__(self, checkpoint_store: CheckpointStore):
        self._checkpoint_store = checkpoint_store
        self._subscription = None

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def name(self) -> str:
        return self.get_name()

    @property
    def subscription(self) -> ApplicationSubscription:
        return self._subscription

    @abstractmethod
    async def process_event(self, event: DomainEvent) -> None:
        """Reimplement to provide reaction to the specific domain event"""
        raise NotImplementedError

    async def _consume_subscription(self) -> None:
        # TODO - rethink, maybe there is a better way to consume events???
        # TODO - intercept asyncio.CancelledError to shutdown gracefully

        while self.subscription.running:
            application_event = await self.subscription.next_event()

            # TODO - ACHTUNG - dangerous, what if only one operation will be completed??? Should be executed in a single transaction (how??? there are no Trasactions in event sourcing)
            # TODO - ORLY - add unit of work to coordinate event processing???
            await self.process_event(event=application_event.domain_event)
            await self.checkpoint_store.set_checkpoint(name=self.subscription.subscription_id, value=application_event.commit_position)

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    def set_subscription(self, subscription: ApplicationSubscription) -> asyncio.Task:
        """Only one subscription currently allowed"""
        # TODO - refactor to handle multiple subscriptions
        # TODO - there should be another way to add subscriptions, for example 'add_subscription'

        if self.subscription is not None:
            raise ValueError(f'Failed to set subscription for {self.name} - only one subspcription can be set for event processor.')
        self._subscription = subscription
        task = asyncio.create_task(self._consume_subscription(), name=f'{self.name}_subscription_task')
        return task

    async def stop(self) -> None:
        await self.subscription.stop()


class Application(EventProcessor):
    """
    The next step after you define your aggregates is to write an application.
    Aggregates serve as a central place for business logic, but they are nothing on their own. Aggregates should be able to communicate with other aggregates and be a part
    of use cases. This is where applcation comes in.

    Application is the place where you implement your use cases, starting from simple things like creation of aggregates and ending with complex cases which can involve
    interaction between multiple aggregates.
    """

    def __init__(self, event_store: EventStore, checkpoint_store: CheckpointStore):
        super().__init__(checkpoint_store=checkpoint_store)
        self._repository = AsyncRepository(event_store=event_store)
        self._register_command_handlers()

    @property
    def repository(self) -> AsyncRepository:
        return self._repository

    @staticmethod
    def _extract_type(fn: Callable) -> Type:
        signature = inspect.signature(fn)
        params = list(signature.parameters.values())
        if params:
            extracted_type = params[-1].annotation
            return extracted_type
        else:
            raise ValueError('No parameters found in signature')

    def _register_command_handlers(self) -> None:
        for attribute in dir(self):
            if attribute.startswith('__'):
                continue
            attribute = getattr(self, attribute)
            if inspect.ismethod(attribute):
                is_command_handler = getattr(attribute, 'is_command_handler', None)  # TODO - fragile
                if is_command_handler:
                    command_type = self._extract_type(attribute)
                    if not issubclass(command_type, Command):
                        raise TypeError(f'Invalid command type, expected <Command>, got {type(command_type)}')
                    mapper.register_command_handler(command_type=command_type, handler=attribute)
                    logger.debug(f'Command handler {attribute} registered for command {command_type}')


class Projection(EventProcessor):
    # TODO - Projections should give a possibility to provide stats about how it is doing
    # TODO - Projection stats should be accessed through REST API http://localhost/projectionStats/CountByDocument

    @abstractmethod
    def drop(self) -> None:
        """There should be functionality to drop projection and rebuild it from scratch"""
        raise NotImplementedError

    async def initialize(self) -> None:
        pass
