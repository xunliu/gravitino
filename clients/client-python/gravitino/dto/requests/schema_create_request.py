from dataclasses import dataclass
from typing import Optional, Dict

from gravitino.rest.rest_message import RESTRequest


@dataclass
class SchemaCreateRequest(RESTRequest):

    name: str
    comment: Optional[str]
    properties: Optional[Dict[str, str]]

    # def __init__(self, name: str, comment: Optional[str], properties: Optional[Dict[str, str]]):
    #     self.name = name
    #     self.comment = comment
    #     self.properties = properties

    def validate(self):
        assert self.name is not None, "\"name\" field is required and cannot be empty"
