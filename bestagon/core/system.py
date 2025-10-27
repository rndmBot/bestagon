import logging
from abc import ABC, abstractmethod

from bestagon.core.message_bus import AsyncCommandBus, AsyncQueryBus


logger = logging.getLogger(__name__)


class EventSourcedSystem(ABC):
    # TODO - application graph
    # TODO - split workflow into two parts - application workflow (write side) and projection workflow (read side)

    # TODO - how to automatically register command handlers???
    # TODO - how to automatically register query handlers???

    def __init__(self):
        self._command_bus = AsyncCommandBus()
        self._query_bus = AsyncQueryBus()

    @property
    def command_bus(self) -> AsyncCommandBus:
        return self._command_bus

    @property
    def query_bus(self) -> AsyncQueryBus:
        return self._query_bus

    @abstractmethod
    def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        raise NotImplementedError
