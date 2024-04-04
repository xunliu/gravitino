from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from gravitino.rest import RESTMessage
from gravitino.rest.rest_response import RESTResponse


@dataclass
class BaseResponse(RESTResponse):
    code: int

    def __init__(self, code: int):
        self.code = code

    @classmethod
    def default(cls):
        return cls(code=0)

    def validate(self):
        if self.code < 0:
            raise ValueError("code must be >= 0")