"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from abc import abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.metalake_change import MetalakeChange
from gravitino.api.schema_change import SchemaChange


@dataclass
class SchemaUpdateRequestType(DataClassJsonMixin):
    type: str = field(metadata=config(field_name='@type'))

    def __init__(self, type: str):
        self.type = type


@dataclass
class SchemaUpdateRequest:
    """Represents an interface for Metalake update requests."""

    # @abstractmethod
    # def validate(self):
    #     pass

    # @abstractmethod
    # def metalake_change(self):
    #     pass

    @dataclass
    class SetSchemaPropertyRequest(SchemaUpdateRequestType):
        """Represents a request to set a property on a Metalake."""

        property: str = None
        """The property to set."""

        value: str = None
        """The value of the property."""

        def __init__(self, property: str, value: str):
            super().__init__("setProperty")
            self.property = property
            self.value = value

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property or value are not set.
            """
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self.value:
                raise ValueError('"value" field is required and cannot be empty')

        # def schema_change(self):
        #     return SchemaChange.set_property(self.property, self.value)

    @dataclass
    class RemoveSchemaPropertyRequest(SchemaUpdateRequestType):
        """Represents a request to remove a property from a Metalake."""

        property: str = None
        """The property to remove."""

        def __init__(self, property: str):
            super().__init__("removeProperty")
            self.property = property

        def validate(self):
            """Validates the fields of the request.

            Raises:
                 IllegalArgumentException if property is not set.
            """
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')

        def schema_change(self):
            return SchemaChange.remove_property(self.property)
