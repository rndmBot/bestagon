class AggregateNotFoundError(Exception):
    pass


class AggregateIDMismatch(Exception):
    pass


class AggregateVersionError(Exception):
    pass


class ApplicationError(Exception):
    pass


class IntegrityError(Exception):
    pass


class InvalidPositionError(Exception):
    pass


class ValidationError(Exception):
    pass
