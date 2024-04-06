from dataclasses import dataclass
from typing import Optional, Dict
from abc import ABC, abstractmethod

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.audit_dto import AuditDTO


# from audit_dto import AuditDTO


# from objects import Objects
# from preconditions import Preconditions

@dataclass
class MetalakeDTO(DataClassJsonMixin):
    name: str
    comment: Optional[str]
    properties: Optional[Dict[str, str]]
    audit: AuditDTO = None

    # def __init__(self, name: str, comment: str, properties: Dict[str, str], audit: AuditDTO = None):
    #     self.name = name
    #     self.comment = comment
    #     self.properties = properties
    #     self.audit = audit

    def name(self):
        return self.name

    def comment(self):
        return self.comment

    def properties(self):
        return self.properties

    def auditInfo(self):
        return self.audit

    @classmethod
    def builder(cls):
        return cls.Builder()

    def equals(self, other):
        if self == other:
            return True
        if not isinstance(other, MetalakeDTO):
            return False
        return self.name == other.name and self.comment == other.comment and \
            self.property_equal(self.properties, other.properties) and self.audit == other.audit

    def property_equal(self, p1, p2):
        if p1 is None and p2 is None:
            return True
        if p1 is not None and not p1 and p2 is None:
            return True
        if p2 is not None and not p2 and p1 is None:
            return True
        return p1 == p2

    # def __hash__(self):
    #     return Objects.hash(self.name, self.comment, self.audit)

    class Builder:
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
            # Preconditions.check_argument(self.name is not None and self.name.strip(), "name cannot be null or empty")
            # Preconditions.check_argument(self.audit is not None, "audit cannot be null")
            return MetalakeDTO(self.name, self.comment, self.properties, self.audit)

# from dataclasses import dataclass
# from typing import Optional, Dict
#
# from gravitino.dto.audit_dto import AuditDTO
# from gravitino.metalake import Metalake
#
# from dataclasses import dataclass
# from dataclasses_json import DataClassJsonMixin
#
# @dataclass
# class MetalakeDTO(DataClassJsonMixin):
#     name: str
#     comment: Optional[str]
#     properties: Optional[Dict[str, str]]
#     audit: AuditDTO
#
#     def name(self):
#         return self.name
#
#     def comment(self):
#         return self.comment
#
#     def properties(self):
#         return self.properties
#
#     def audit_info(self):
#         return self.audit
#
#     @staticmethod
#     def builder():
#         return MetalakeDTOBuilder()
#
# class MetalakeDTOBuilder:
#     def __init__(self):
#         self.name = None
#         self.comment = None
#         self.properties = None
#         self.audit = None
#
#     def with_name(self, name):
#         self.name = name
#         return self
#
#     def with_comment(self, comment):
#         self.comment = comment
#         return self
#
#     def with_properties(self, properties):
#         self.properties = properties
#         return self
#
#     def with_audit(self, audit):
#         self.audit = audit
#         return self
#
#     def build(self):
#         if self.name is None or not self.name:
#             raise ValueError("name cannot be null or empty")
#         if self.audit is None:
#             raise ValueError("audit cannot be null")
#         return MetalakeDTO(self.name, self.comment, self.properties, self.audit)
#
#     def __eq__(self, other):
#         if not isinstance(other, MetalakeDTOBuilder):
#             return False
#         return self.name == other.name and self.comment == other.comment and self.properties == other.properties and self.audit == other.audit
#
#     def __hash__(self):
#         return hash((self.name, self.comment, self.audit))
