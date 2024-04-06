from gravitino.dto.responses.base_response import BaseResponse


class DropResponse(BaseResponse):
    dropped : bool
    # def __init__(self, dropped: bool):
    #     super().__init__(0)
    #     self.dropped = dropped

    def dropped(self) -> bool:
        return self.dropped

    # @classmethod
    # def from_json(cls, json_data):
    #     # Custom method to deserialize DropResponse object from JSON
    #     dropped = json_data.get('dropped', False)
    #     return cls(dropped)
