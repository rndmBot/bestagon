from dataclasses import dataclass


@dataclass(frozen=True)
class Command:
    """
    A fundamental concept of event sourcing that is easily overlooked: commands are inextricably linked to the state of the world at the time the command was created.
    Even more plainly: never enqueue commands. The instant that command is enqueued it becomes irrelevant because the state of the world has moved on.
    Instead of enqueuing them, commands should be be processed via request/reply. A live service should handle the command request, validate it as of the current state,
    and reject it accordingly or return a list of events. Not only does this give our application the chance to get more robust error messages as to why a command was rejected,
    but it also ensures that bad commands can't produce events. There will never exist an event produced from a stale command.

    Source: https://blog.cosmonic.com/engineering/commands-are-not-real/
    """
    # TODO - commands sent through a command bus should allow the result to be returned
    pass


@dataclass(frozen=True)
class Query:
    pass