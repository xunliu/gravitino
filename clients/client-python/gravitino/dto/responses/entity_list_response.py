from dataclasses import dataclass, field
from typing import Optional, List

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.name_identifier import NameIdentifier


@dataclass
class EntityListResponse(BaseResponse):
    idents: Optional[List[NameIdentifier]] = field(metadata=config(field_name='identifiers'))

    # def __init__(self):
    #     super().__init__()
    #     if self.idents is None:
    #         raise ValueError("identifiers must not be null")

    def validate(self):
        super().validate()

        assert self.idents is not None, "identifiers must not be null"