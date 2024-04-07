from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin

from gravitino.rest.rest_response import RESTResponse


@dataclass
class BaseResponse(RESTResponse):
    """
    Represents a base response for REST API calls.
    """
    code: int

    @classmethod
    def default(cls):
        return cls(code=0)

    def validate(self):
        """
        Validates the response code.
        TODO: @throws IllegalArgumentException if code value is negative.
        """
        if self.code < 0:
            raise ValueError("code must be >= 0")