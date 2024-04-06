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

        # Preconditions.check_argument(self.metalake is not None, "metalake must not be null")
        # Preconditions.check_argument(StringUtils.isNotBlank(self.metalake.name()), "metalake 'name' must not be null and empty")
        # Preconditions.check_argument(self.metalake.auditInfo() is not None, "metalake 'audit' must not be null")
