"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from abc import ABC
from dataclasses import dataclass
from typing import Optional, Dict

from gravitino.dto.audit_dto import AuditDTO
from gravitino.metalake import Metalake


@dataclass
class MetalakeDTO(Metalake):  # DataClassJsonMixin,
    """
    Represents a Metalake Data Transfer Object (DTO) that implements the Metalake interface.
    """
    name: str  # The name of the Metalake DTO.
    comment: Optional[str]  # The comment of the Metalake DTO.
    properties: Optional[Dict[str, str]]  # The properties of the Metalake DTO.
    audit: AuditDTO = None  # The audit information of the Metalake DTO.

    # @property
    def name(self):
        return self.name

    # @property
    def comment(self):
        return self.comment

    # @property
    def properties(self):
        return self.properties

    # @property
    def audit_info(self):
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

    class Builder:
        """
        A builder class for constructing instances of MetalakeDTO.
        Args:
            The type of the builder subclass.
        """

        def __init__(self):
            self.name = None
            self.comment = None
            self.properties = None
            self.audit = None

        def with_name(self, name):
            """
            Sets the name of the Metalake DTO.
            """
            self.name = name
            return self

        def with_comment(self, comment):
            """
            Sets the comment of the Metalake DTO.
            """
            self.comment = comment
            return self

        def with_properties(self, properties):
            """
            Sets the properties of the Metalake DTO.
            """
            self.properties = properties
            return self

        def with_audit(self, audit):
            """
            Sets the audit information of the Metalake DTO.
            """
            self.audit = audit
            return self

        def build(self):
            """
            Builds an instance of MetalakeDTO using the builder's properties.
            """
            assert self.name is not None and self.name.strip(), "name cannot be null or empty"
            assert self.audit is not None, "audit cannot be null"
            return MetalakeDTO(self.name, self.comment, self.properties, self.audit)
