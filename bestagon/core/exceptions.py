class BestagonError(Exception):
    pass


class AggregateTypeAlreadyRegisteredError(BestagonError):
    pass


class AggregateTypeNotRegisteredError(BestagonError):
    pass


class AggregateVersionError(BestagonError):
    pass


class CanonicAggregateTypeAlreadyRegisteredError(BestagonError):
    pass


class CanonicAggregateTypeNotRegisteredError(BestagonError):
    pass


class CanonicEventTypeAlreadyRegisteredError(BestagonError):
    pass


class CanonicEventTypeNotRegistered(BestagonError):
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


class IntegrityError(BestagonError):
    pass


class LegacyAggregateTypeAlreadyRegisteredError(BestagonError):
    pass


class LegacyEventTypeAlreadyRegisteredError(BestagonError):
    pass


class QueryHandlerAlreadyRegisteredError(BestagonError):
    pass


class QueryHandlerNotRegisteredError(BestagonError):
    pass
