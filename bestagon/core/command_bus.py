from abc import ABC, abstractmethod
from typing import Type, Callable

from bestagon.domain.command import Command
from bestagon.exceptions import HandlerNotFound


class CommandBus(ABC):
    # TODO - command bus can contain middleware

    @abstractmethod
    def execute(self, command: Command) -> None:
        raise NotImplementedError

    @abstractmethod
    def register_command_handler(self, command_type: Type[Command], handler: Callable[[Command], None]) -> None:
        raise NotImplementedError


class SimpleCommandBus(CommandBus):
    def __init__(self):
        self._handler_map = dict()

    def execute(self, command: Command) -> None:
        handler = self._handler_map.get(type(command))
        if handler is None:
            raise HandlerNotFound(f'No handler found for command {type(command)}')
        handler(command)

    def register_command_handler(self, command_type: Type[Command], handler: Callable[[Command], None]) -> None:
        # Command can have one and only one handler
        self._handler_map[command_type] = handler
