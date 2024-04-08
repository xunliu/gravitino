from dataclasses import dataclass
from typing import Optional

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.schema_dto import SchemaDTO


@dataclass
class SchemaResponse(BaseResponse, DataClassJsonMixin):
    schema: Optional[SchemaDTO]

    # def __init__(self, schema: Optional[SchemaDTO]):
    #     super().__init__(0)
    #     self.schema = schema

    def validate(self):
        super().validate()

        assert self.schema is not None, "schema must be non-null"
        assert self.schema.name is not None, "schema 'name' must not be null and empty"
        assert self.schema.audit is not None, "schema 'audit' must not be null"
