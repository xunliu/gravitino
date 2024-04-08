from dataclasses import dataclass, field
from typing import Optional, Dict

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.fileset import Fileset


@dataclass
class FilesetCreateRequest(DataClassJsonMixin):
    name: str
    comment: Optional[str]
    type: Fileset.Type
    storage_location: str = field(metadata=config(field_name='storageLocation'))
    properties: Dict[str, str]

    # def __init__(self, name: str, comment: Optional[str], type: Optional[Fileset.Type], storage_location: Optional[str], properties: Optional[Dict[str, str]]):
    #     self.name = name
    #     self.comment = comment
    #     self.type = type
    #     self.storage_location = storage_location
    #     self.properties = properties

    def validate(self):
        if not self.name:
            raise ValueError('"name" field is required and cannot be empty')
