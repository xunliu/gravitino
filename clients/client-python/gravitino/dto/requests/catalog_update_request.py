from dataclasses import field, dataclass
from typing import Optional

from dataclasses_json import config, DataClassJsonMixin


@dataclass
class CatalogUpdateRequestType(DataClassJsonMixin):
    type: str = field(metadata=config(field_name='@type'))

    def __init__(self, type: str):
        self.type = type


class CatalogUpdateRequest:
    class RenameCatalogRequest(CatalogUpdateRequestType):
        new_name: Optional[str] = field(metadata=config(field_name='newName'))

        def validator(self):
            assert self.new_name is None, '"newName" field is required and cannot be empty'

    class UpdateCatalogCommentRequest(CatalogUpdateRequestType):
        new_comment: Optional[str] = field(metadata=config(field_name='newComment'))

        def validator(self):
            assert self.new_comment is None, '"newComment" field is required and cannot be empty'

    class SetCatalogPropertyRequest(CatalogUpdateRequestType):
        property: Optional[str] = None
        value: Optional[str] = None

        def validator(self):
            assert self.property is None, "\"property\" field is required and cannot be empty"
            assert self.value is None, "\"value\" field is required and cannot be empty"

    class RemoveCatalogPropertyRequest(CatalogUpdateRequestType):
        property: Optional[str] = None

        def validator(self):
            assert self.property is None, "\"property\" field is required and cannot be empty"

