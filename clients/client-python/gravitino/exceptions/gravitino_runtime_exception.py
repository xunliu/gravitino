class GravitinoRuntimeException(RuntimeError):

    def __init__(self, message, *args):
        super().__init__(message.format(*args))

    def __init__(self, cause, message, *args):
        super().__init__(message.format(*args), cause)