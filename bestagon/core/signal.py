from typing import List, Callable, Tuple


class Signal:
    def __init__(self):
        self._callbacks: List[Callable] = list()

    @property
    def callbacks(self) -> Tuple[Callable, ...]:
        return tuple(self._callbacks)

    def connect(self, func: Callable) -> None:
        self._callbacks.append(func)

    def disconnect(self, func: Callable) -> None:
        if func in self.callbacks:
            self._callbacks.remove(func)

    def disconnect_all(self) -> None:
        self._callbacks.clear()

    def emit(self, *args, **kwargs) -> None:
        for callback in self.callbacks:
            callback(*args, **kwargs)
