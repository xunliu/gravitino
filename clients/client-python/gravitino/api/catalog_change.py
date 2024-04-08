from abc import ABC, abstractmethod

class CatalogChange(ABC):

    @staticmethod
    def rename(new_name):
        return CatalogChange.RenameCatalog(new_name)

    @staticmethod
    def update_comment(new_comment):
        return CatalogChange.UpdateCatalogComment(new_comment)

    @staticmethod
    def set_property(property, value):
        return CatalogChange.SetProperty(property, value)

    @staticmethod
    def remove_property(property):
        return CatalogChange.RemoveProperty(property)

    class RenameCatalog:
        def __init__(self, new_name):
            self.new_name = new_name

        def get_new_name(self):
            return self.new_name

        def __eq__(self, other):
            if not isinstance(other, CatalogChange.RenameCatalog):
                return False
            return self.new_name == other.new_name

        def __hash__(self):
            return hash(self.new_name)

        def __str__(self):
            return f"RENAMECATALOG {self.new_name}"

    class UpdateCatalogComment:
        def __init__(self, new_comment):
            self.new_comment = new_comment

        def get_new_comment(self):
            return self.new_comment

        def __eq__(self, other):
            if not isinstance(other, CatalogChange.UpdateCatalogComment):
                return False
            return self.new_comment == other.new_comment

        def __hash__(self):
            return hash(self.new_comment)

        def __str__(self):
            return f"UPDATECATALOGCOMMENT {self.new_comment}"

    class SetProperty:
        def __init__(self, property, value):
            self.property = property
            self.value = value

        def get_property(self):
            return self.property

        def get_value(self):
            return self.value

        def __eq__(self, other):
            if not isinstance(other, CatalogChange.SetProperty):
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
            if not isinstance(other, CatalogChange.RemoveProperty):
                return False
            return self.property == other.property

        def __hash__(self):
            return hash(self.property)

        def __str__(self):
            return f"REMOVEPROPERTY {self.property}"
