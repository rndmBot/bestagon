from abc import ABC, abstractmethod


class CheckpointStore(ABC):
    # TODO - add possibility to remove all checkpoints for specific application

    @abstractmethod
    async def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def delete_checkpoint(self, name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_checkpoint(self, name: str) -> int:
        raise NotImplementedError

    @abstractmethod
    async def initialize(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def set_checkpoint(self, name: str, value: int) -> None:
        raise NotImplementedError
