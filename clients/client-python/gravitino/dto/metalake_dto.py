from dataclasses import dataclass
from typing import Optional, Dict

from gravitino.dto.audit_dto import AuditDTO
from gravitino.metalake import Metalake


# @dataclass
class MetalakeDTO(Metalake):
    name: str
    comment: Optional[str]
    properties: Optional[Dict[str, str]]
    audit: AuditDTO

    def name(self):
        return self.name

    def comment(self):
        return self.comment

    def properties(self):
        return self.properties

    def audit_info(self):
        return self.audit

    @staticmethod
    def builder():
        return MetalakeDTOBuilder()

class MetalakeDTOBuilder:
    def __init__(self):
        self.name = None
        self.comment = None
        self.properties = None
        self.audit = None

    def with_name(self, name):
        self.name = name
        return self

    def with_comment(self, comment):
        self.comment = comment
        return self

    def with_properties(self, properties):
        self.properties = properties
        return self

    def with_audit(self, audit):
        self.audit = audit
        return self

    def build(self):
        if self.name is None or not self.name:
            raise ValueError("name cannot be null or empty")
        if self.audit is None:
            raise ValueError("audit cannot be null")
        return MetalakeDTO(self.name, self.comment, self.properties, self.audit)

    def __eq__(self, other):
        if not isinstance(other, MetalakeDTOBuilder):
            return False
        return self.name == other.name and self.comment == other.comment and self.properties == other.properties and self.audit == other.audit

    def __hash__(self):
        return hash((self.name, self.comment, self.audit))
