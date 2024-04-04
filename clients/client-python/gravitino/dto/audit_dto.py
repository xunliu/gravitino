from dataclasses import dataclass
from typing import Optional
from datetime import datetime

from gravitino.audit import Audit


@dataclass
class AuditDTO(Audit):
    creator: str
    create_time: datetime
    last_modifier: str
    last_modified_time: datetime

    def creator(self):
        return self.creator

    def create_time(self):
        return self.create_time

    def last_modifier(self):
        return self.last_modifier

    def last_modified_time(self):
        return self.last_modified_time

    @staticmethod
    def builder():
        return AuditDTOBuilder()

class AuditDTOBuilder:
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
        if self.creator is None:
            raise ValueError("creator cannot be None")
        if self.create_time is None:
            raise ValueError("create_time cannot be None")
        if self.last_modifier is None:
            raise ValueError("last_modifier cannot be None")
        if self.last_modified_time is None:
            raise ValueError("last_modified_time cannot be None")
        return AuditDTO(self.creator, self.create_time, self.last_modifier, self.last_modified_time)
