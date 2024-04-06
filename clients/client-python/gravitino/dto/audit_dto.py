from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from dataclasses_json import DataClassJsonMixin


@dataclass
class AuditDTO(DataClassJsonMixin):
    creator: str
    create_time: str #datetime #
    last_modifier: str
    last_modified_time: str #datetime #

    # def __init__(self, creator: str, create_time: str, last_modifier: str, last_modified_time: str):
    #     self.creator = creator
    #     self.create_time = create_time
    #     self.last_modifier = last_modifier
    #     self.last_modified_time = last_modified_time

    def creator(self):
        return self.creator

    def create_time(self):
        return self.create_time

    def last_modifier(self):
        return self.last_modifier

    def last_modified_time(self):
        return self.last_modified_time

    # @classmethod
    # def from_json(cls, json_data):
    #     create_time_str = json_data.get('create_time')
    #     create_time_obj = datetime.fromisoformat(create_time_str.replace('Z', '+00:00'))
    #     return cls(create_time=create_time_obj)

    @classmethod
    def builder(cls):
        return cls.Builder()

    class Builder:
        def __init__(self):
            self.creator = None
            self.create_time = None
            self.last_modifier = None
            self.last_modified_time = None

        def with_creator(self, creator):
            self.creator = creator
            return self

        def with_create_time(self, create_time):
            self.create_time = create_time
            return self

        def with_last_modifier(self, last_modifier):
            self.last_modifier = last_modifier
            return self

        def with_last_modified_time(self, last_modified_time):
            self.last_modified_time = last_modified_time
            return self

        def build(self):
            return AuditDTO(self.creator, self.create_time, self.last_modifier, self.last_modified_time)


# from dataclasses import dataclass
# from typing import Optional
# from datetime import datetime
#
# from dataclasses_json import DataClassJsonMixin
#
# from gravitino.audit import Audit
#
#
# @dataclass
# class AuditDTO(DataClassJsonMixin):
#     creator: str
#     create_time: datetime
#     last_modifier: str
#     last_modified_time: datetime
#
#     def creator(self):
#         return self.creator
#
#     def create_time(self):
#         return self.create_time
#
#     def last_modifier(self):
#         return self.last_modifier
#
#     def last_modified_time(self):
#         return self.last_modified_time
#
#     @staticmethod
#     def builder():
#         return AuditDTOBuilder()
#
# class AuditDTOBuilder:
#     def __init__(self):
#         self.creator = None
#         self.create_time = None
#         self.last_modifier = None
#         self.last_modified_time = None
#
#     def with_creator(self, creator):
#         self.creator = creator
#         return self
#
#     def with_create_time(self, create_time):
#         self.create_time = create_time
#         return self
#
#     def with_last_modifier(self, last_modifier):
#         self.last_modifier = last_modifier
#         return self
#
#     def with_last_modified_time(self, last_modified_time):
#         self.last_modified_time = last_modified_time
#         return self
#
#     def build(self):
#         if self.creator is None:
#             raise ValueError("creator cannot be None")
#         if self.create_time is None:
#             raise ValueError("create_time cannot be None")
#         if self.last_modifier is None:
#             raise ValueError("last_modifier cannot be None")
#         if self.last_modified_time is None:
#             raise ValueError("last_modified_time cannot be None")
#         return AuditDTO(self.creator, self.create_time, self.last_modifier, self.last_modified_time)
