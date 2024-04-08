from dataclasses import Field, dataclass
from typing import Optional

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.fileset_dto import FilesetDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class FilesetResponse(BaseResponse):
    fileset: FilesetDTO

    # @validator('fileset')
    # def fileset_must_not_be_null(cls, v):
    #     assert v is not None, 'fileset must not be null'
    #     return v
    #
    # @validator('fileset', each_item=True)
    # def fileset_fields_must_be_valid(cls, v):
    #     assert v.name is not None and v.name != '', "fileset 'name' must not be null and empty"
    #     assert v.storage_location is not None and v.storage_location != '', "fileset 'storageLocation' must not be null and empty"
    #     assert v.type is not None, "fileset 'type' must not be null and empty"
    #     return v

    # 假设 BaseResponse 有一个 validate 方法
    def validate(self):
        super().validate()
        assert self.fileset is not None, "fileset must not be null"
        assert self.fileset.name, "fileset 'name' must not be null and empty"
        assert self.fileset.storage_location, "fileset 'storageLocation' must not be null and empty"
        assert self.fileset.type is not None, "fileset 'type' must not be null and empty"
