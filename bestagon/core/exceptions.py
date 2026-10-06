class BestagonError(Exception):
    pass


class HandlerAlreadyRegistered(BestagonError):
    pass


class TypeNotRegisteredError(BestagonError):
    pass


