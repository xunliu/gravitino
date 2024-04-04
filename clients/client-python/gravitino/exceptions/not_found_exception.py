from gravitino.exceptions.gravitino_runtime_exception import GravitinoRuntimeException


class NotFoundException(GravitinoRuntimeException):

    def __init__(self, message, *args):
        super().__init__(message, *args)

    def __init__(self, cause, message, *args):
        super().__init__(cause, message, *args)