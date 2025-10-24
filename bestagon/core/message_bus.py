import logging
from abc import ABC, abstractmethod
from typing import Type, Callable

from bestagon.core.message import Command, Query
from bestagon.exceptions import HandlerNotFound, HandlerAlreadyRegistered

logger = logging.getLogger(__name__)


class CommandBus(ABC):
    # TODO - command bus can contain middleware

    @abstractmethod
    def execute(self, command: Command) -> None:
        # TODO - think about returning command result (executed, failed etc.)
        raise NotImplementedError

    @abstractmethod
    def get_command_handler(self, command_type: Type[Command]) -> Callable:
        raise NotImplementedError

    @abstractmethod
    def register_command_handler(self, command_type: Type[Command], handler: Callable) -> None:
        raise NotImplementedError


class QueryBus(ABC):
    # TODO - can contain middleware
    # TODO - there are multiple types of queries - https://docs.axoniq.io/axon-framework-reference/4.11/queries/

    @abstractmethod
    def execute(self, query: Query) -> any:
        raise NotImplementedError

    @abstractmethod
    def get_query_handler(self, query_type: Type[Query]) -> Callable:
        raise NotImplementedError

    @abstractmethod
    def register_query_handler(self, query_type: Type[Query], query_handler: Callable) -> None:
        raise NotImplementedError


class AsyncCommandBus(CommandBus):
    def __init__(self):
        self._handler_map = dict()

    async def execute(self, command: Command) -> None:
        if not isinstance(command, Command):
            raise TypeError(f'Invalid command type, expected <Command>, got {type(command)}')

        handler = self.get_command_handler(command_type=type(command))
        await handler(command)

    def get_command_handler(self, command_type: Type[Command]) -> Callable:
        if not issubclass(command_type, Command):
            raise TypeError(f'Invalid command type, expected subclass <Command>, got {command_type}')

        handler = self._handler_map.get(command_type)
        if handler is None:
            raise HandlerNotFound(f'No handler defined for command {command_type}')

        return handler

    def register_command_handler(self, command_type: Type[Command], handler: Callable) -> None:
        if not issubclass(command_type, Command):
            raise TypeError(f'Invalid command type, expected subclass <Command>, got {command_type}')
        if command_type in self._handler_map:
            raise ValueError(f'Handler for command {command_type} have already bee registered.')

        # Command can have one and only one handler
        self._handler_map[command_type] = handler


class AsyncQueryBus(QueryBus):
    def __init__(self):
        self._handler_map = dict()

    def get_query_handler(self, query_type: Type[Query]) -> Callable:
        handler = self._handler_map.get(query_type)
        if handler is None:
            raise HandlerNotFound(f'No handler registered for query {query_type}')
        return handler

    def register_query_handler(self, query_type: Type[Query], query_handler: Callable) -> None:
        if not issubclass(query_type, Query):
            raise TypeError(f'Invalid query type, expercted <Query>, got {query_type}')

        try:
            _ = self.get_query_handler(query_type)
            raise HandlerAlreadyRegistered(f'Handler for query {query_type} have already been registered')
        except HandlerNotFound:
            pass

        self._handler_map[query_type] = query_handler

    async def execute(self, query: Query) -> any:
        handler = self.get_query_handler(query_type=type(query))
        query_result = await handler(query)
        return query_result
