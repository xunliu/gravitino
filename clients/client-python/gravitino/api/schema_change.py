from abc import ABC, abstractmethod


class SchemaChange(ABC):

    @staticmethod
    def set_property(property, value):
        return SchemaChange.SetProperty(property, value)

    @staticmethod
    def remove_property(property):
        return SchemaChange.RemoveProperty(property)

    class SetProperty:
        def __init__(self, property, value):
            self.property = property
            self.value = value

        def get_property(self):
            return self.property

        def get_value(self):
            return self.value

        def __eq__(self, other):
            if not isinstance(other, SchemaChange.SetProperty):
                return False
            return self.property == other.property and self.value == other.value

        def __hash__(self):
            return hash((self.property, self.value))

        def __str__(self):
            return f"SETPROPERTY {self.property} {self.value}"

    class RemoveProperty:
        def __init__(self, property):
            self.property = property

        def get_property(self):
            return self.property

        def __eq__(self, other):
            if not isinstance(other, SchemaChange.RemoveProperty):
                return False
            return self.property == other.property

        def __hash__(self):
            return hash(self.property)

        def __str__(self):
            return f"REMOVEPROPERTY {self.property}"
