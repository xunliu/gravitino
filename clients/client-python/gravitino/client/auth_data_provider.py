from abc import ABC, abstractmethod
from io import IOBase

class AuthDataProvider(IOBase):

    @abstractmethod
    def has_token_data(self) -> bool:
        return False

    @abstractmethod
    def get_token_data(self) -> bytes:
        return None
