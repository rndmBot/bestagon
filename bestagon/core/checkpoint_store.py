from abc import ABC, abstractmethod
from typing import Tuple

from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class Checkpoint:
    name: str
    value: int | None = Field(default=None, ge=0)


class CheckpointStore(ABC):
    """
    Abstract interface for checkpoint store.
    The checkpoint store's main purpose is to store the position of a last processed event for the application.
    This class should not be instantiated directly, instead it should be inherited to create a database-specific implementation
    of checkpoint store by implementing all abstract methods.
    """

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def delete_checkpoint(self, name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_checkpoint(self, name: str) -> Checkpoint:
        raise NotImplementedError

    @abstractmethod
    async def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_checkpoints(self) -> Tuple[Checkpoint, ...]:
        raise NotImplementedError

    @abstractmethod
    async def set_checkpoint(self, checkpoint: Checkpoint) -> None:
        raise NotImplementedError
