from abc import ABC, abstractmethod
from typing import Union


class CheckpointStore(ABC):
    @abstractmethod
    def get_checkpoint(self, name: str) -> Union[int, None]:
        raise NotImplementedError

    @abstractmethod
    def set_checkpoint(self, name: str, value: int) -> None:
        raise NotImplementedError
