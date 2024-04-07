from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from dataclasses_json import DataClassJsonMixin


@dataclass
class AuditDTO(DataClassJsonMixin):
    """
    Data transfer object representing audit information.
    """
    creator: str
    create_time: str  # TODO: Can't deserialized datetime from JSON
    last_modifier: str
    last_modified_time: str  # TODO: Can't deserialized datetime from JSON

    def creator(self):
        return self.creator

    def create_time(self):
        return self.create_time

    def last_modifier(self):
        return self.last_modifier

    def last_modified_time(self):
        return self.last_modified_time

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
