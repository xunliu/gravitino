class IllegalNameIdentifierException(Exception):

    def __init__(self, message=None, *args):
        if message:
            super().__init__(message.format(*args))
        else:
            super().__init__()

    def __init__(self, cause=None, message=None, *args):
        if message:
            super().__init__(message.format(*args), cause)
        else:
            super().__init__(cause)

    def __init__(self, cause=None):
        if cause:
            super().__init__(cause)
        else:
            super().__init__()

    def __init__(self):
        super().__init__()