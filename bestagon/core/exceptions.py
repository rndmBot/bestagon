class DomainException(Exception):
    pass


class BestagonError(Exception):
    pass


class AggregateNotFoundError(BestagonError):
    pass


class AggregateIDMismatch(BestagonError):
    pass


class AggregateVersionError(BestagonError):
    pass


class IntegrityError(BestagonError):
    pass


class HandlerNotFound(BestagonError):
    pass


class HandlerAlreadyRegistered(BestagonError):
    pass


class TypeNotRegisteredError(BestagonError):
    pass


class TypeAlreadyRegisteredError(BestagonError):
    pass
