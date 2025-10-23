import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Type, Callable

from bestagon.core.message import Command
from bestagon.exceptions import HandlerNotFound


logger = logging.getLogger(__name__)


class CommandBus(ABC):
    # TODO - command bus can contain middleware

    @abstractmethod
    def execute(self, command: Command) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_command_handler(self, command_type: Type[Command]) -> Callable:
        raise NotImplementedError

    @abstractmethod
    def register_command_handler(self, command_type: Type[Command], handler: Callable) -> None:
        raise NotImplementedError


class AsyncCommandBus(CommandBus):
    def __init__(self):
        self.queue = asyncio.Queue()
        self.shutdown_timeout_sec = 10
        self._handler_map = dict()
        self._running = False
        self._shutdown = False  # TODO - Python 3.10 compatibility, should be replaced with queue.shutdown()
        self._start_task = None

    async def _process_queue(self) -> None:
        if self.is_running():
            return

        logger.info('Starting command bus.')
        self._running = True
        self._shutdown = False
        while self.is_running():
            command = await self.queue.get()
            command_handler = self.get_command_handler(type(command))
            logger.debug(f'Handling command {command} with handler {command_handler}')
            await command_handler(command)
            logger.debug(f'Command {command} have been handled.')
            self.queue.task_done()

    def execute(self, command: Command) -> None:
        if not isinstance(command, Command):
            raise TypeError(f'Invalid command type, expected <Command>, got {type(command)}')
        if self._shutdown:
            raise RuntimeError('Failed to execute command, shutdown initiated.')

        self.queue.put_nowait(command)
        logger.info(f'Command put on the bus: {command}')

    def get_command_handler(self, command_type: Type[Command]) -> Callable:
        if not issubclass(command_type, Command):
            raise TypeError(f'Invalid command type, expected subclass <Command>, got {command_type}')

        return self._handler_map[command_type]

    def is_running(self) -> bool:
        return self._running

    def register_command_handler(self, command_type: Type[Command], handler: Callable) -> None:
        if not issubclass(command_type, Command):
            raise TypeError(f'Invalid command type, expected subclass <Command>, got {command_type}')
        if command_type in self._handler_map:
            raise ValueError(f'Handler for command {command_type} have already bee registered.')

        # Command can have one and only one handler
        self._handler_map[command_type] = handler

    def start(self) -> None:
        self._start_task = asyncio.create_task(self._process_queue())

    async def stop(self) -> None:
        logger.info('Shutting down command bus...')
        self._shutdown = True
        await asyncio.wait_for(self.queue.join(), self.shutdown_timeout_sec)
        self._running = False
        logger.info('Command bus have been shut down.')
