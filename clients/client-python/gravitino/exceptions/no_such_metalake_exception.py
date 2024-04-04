from gravitino.exceptions.not_found_exception import NotFoundException


class NoSuchMetalakeException(NotFoundException):

    def __init__(self, message, *args):
        super().__init__(message, *args)

    def __init__(self, cause, message, *args):
        super().__init__(cause, message, *args)