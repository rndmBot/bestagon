class BestagonError(Exception):
    pass


class AggregateTypeAlreadyRegisteredError(BestagonError):
    pass


class AggregateTypeNotRegisteredError(BestagonError):
    pass


class AggregateVersionError(BestagonError):
    pass


class CommandHandlerAlreadyRegisteredError(BestagonError):
    pass


class CommandHandlerNotRegisteredError(BestagonError):
    pass


class DomainException(BestagonError):
    pass


class EventTypeAlreadyRegisteredError(BestagonError):
    pass


class EventTypeNotRegisteredError(BestagonError):
    pass


class EventHandlerAlreadyRegisteredError(BestagonError):
    pass


class EventHandlerNotRegisteredError(BestagonError):
    pass


class QueryHandlerAlreadyRegisteredError(BestagonError):
    pass


class QueryHandlerNotRegisteredError(BestagonError):
    pass
