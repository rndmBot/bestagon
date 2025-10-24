class BestagonError(Exception):
    pass


class AggregateNotFoundError(BestagonError):
    pass


class AggregateIDMismatch(BestagonError):
    pass


class AggregateVersionError(BestagonError):
    pass


class ApplicationError(BestagonError):
    pass


class IntegrityError(BestagonError):
    pass


class InvalidPositionError(BestagonError):
    pass


class HandlerNotFound(BestagonError):
    pass


class HandlerAlreadyRegistered(BestagonError):
    pass


class ValidationError(BestagonError):
    pass
