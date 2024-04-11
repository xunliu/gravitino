"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.schema_dto import SchemaDTO


@dataclass
class SchemaResponse(BaseResponse, DataClassJsonMixin):
    """Represents a response for a schema."""
    schema: SchemaDTO

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if catalog identifiers are not set.
        """
        super().validate()

        assert self.schema is not None, "schema must be non-null"
        assert self.schema.name() is not None, "schema 'name' must not be null and empty"
        assert self.schema.audit_info() is not None, "schema 'audit' must not be null"
