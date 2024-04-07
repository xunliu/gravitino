from gravitino.dto.responses.base_response import BaseResponse


class DropResponse(BaseResponse):
    """
    Represents a response for a drop operation.
    """
    dropped : bool

    def dropped(self) -> bool:
        return self.dropped
