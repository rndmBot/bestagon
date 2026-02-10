from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class Checkpoint:
    name: str
    value: int

    def __post_init__(self):
        if not isinstance(self.name, str):
            raise TypeError('Checkpoint name should a string')
        if not self.name:
            raise ValueError('Checkpoint name should be a non empty string')
        if not isinstance(self.value, int):
            raise TypeError('Chckpoint value should be an integer')
        if self.value < 0:
            raise ValueError('Checkpoint value should be a positive integer')


class CheckpointStore(ABC):
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
