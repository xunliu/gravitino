import json
from dataclasses import dataclass
from typing import Optional, Dict
# from rest_request import RESTRequest
from json import JSONDecodeError
from json import JSONEncoder
from json import JSONDecoder

from gravitino.rest.rest_request import RESTRequest

from dataclasses import dataclass

@dataclass
class MetalakeCreateRequest(RESTRequest):
    name: str
    comment: Optional[str]
    properties: Optional[Dict[str, str]]

    def __init__(self, name: str = None, comment: str = None, properties: Dict[str, str] = None):
        super().__init__()

        self.name = name.strip() if name else None
        self.comment = comment.strip() if comment else None


        self.properties = properties

    def validate(self):
        if not self.name:
            raise ValueError("\"name\" field is required and cannot be empty")

# @dataclass
# class MetalakeCreateRequest(RESTRequest):
#     name: str
#     comment: Optional[str]
#     properties: Optional[Dict[str, str]]
#
#     def __init__(self, name: str = None, comment: str = None, properties: Dict[str, str] = None):
#         super().__init__()
#
#         self.name = name.strip() if name else None
#         self.comment = comment.strip() if comment else None
#         self.properties = properties
#
#     def validate(self):
#         if not self.name:
#             raise ValueError("\"name\" field is required and cannot be empty")
#
#     # def object_hook(d):
#     #     return MetalakeCreateRequest(d['name'], d['comment'], d['properties'])
#
#     @classmethod
#     def from_json_string(cls, json_string):
#         data = json.loads(json_string)
#         return cls(data['name'], data['comment'], data['properties'])
#
#     def to_json_string(self):
#         data = {
#             'name': self.name,
#             'comment': self.comment,
#             'properties': self.properties
#         }
#         return json.dumps(data)

