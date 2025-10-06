from abc import ABC, abstractmethod


class CheckpointStore(ABC):
    @abstractmethod
    def get_checkpoint(self, name: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def set_checkpoint(self, name: str, value: int) -> None:
        raise NotImplementedError
