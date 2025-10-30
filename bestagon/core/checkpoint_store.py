from abc import ABC, abstractmethod


class CheckpointStore(ABC):
    # TODO - add method to retreive all checkpoint names for specified application
    # TODO - add method to clear all checkpoints for specified application

    @abstractmethod
    async def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_checkpoint(self, name: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def set_checkpoint(self, name: str, value: int) -> None:
        raise NotImplementedError
