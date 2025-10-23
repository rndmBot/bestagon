from dataclasses import dataclass


@dataclass(frozen=True)
class Message:
    pass


@dataclass(frozen=True)
class Command(Message):
    pass


@dataclass(frozen=True)
class Query(Message):
    pass
