from dataclasses import dataclass
from typing import Optional

from dataclasses_json import DataClassJsonMixin

# from base_response import BaseResponse
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class MetalakeResponse(BaseResponse):
    metalake: Optional[MetalakeDTO]

    # def __init__(self, metalake: MetalakeDTO = None):
    #     super().__init__(code=0)
    #     self.metalake = metalake

    def validate(self):
        super().validate()

        assert self.metalake is not None, "metalake must not be null"
        assert self.metalake.name is not None, "metalake 'name' must not be null and empty"
        assert self.metalake.audit is not None, "metalake 'audit' must not be null"
