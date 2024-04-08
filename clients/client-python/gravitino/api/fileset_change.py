from abc import ABC, abstractmethod

class FilesetChange(ABC):

    @staticmethod
    def rename(new_name):
        return FilesetChange.RenameFileset(new_name)

    @staticmethod
    def update_comment(new_comment):
        return FilesetChange.UpdateFilesetComment(new_comment)

    @staticmethod
    def set_property(property, value):
        return FilesetChange.SetProperty(property, value)

    @staticmethod
    def remove_property(property):
        return FilesetChange.RemoveProperty(property)

    class RenameFileset:
        def __init__(self, new_name):
            self.new_name = new_name

        def get_new_name(self):
            return self.new_name

        def __eq__(self, other):
            if not isinstance(other, FilesetChange.RenameFileset):
                return False
            return self.new_name == other.new_name

        def __hash__(self):
            return hash(self.new_name)

        def __str__(self):
            return f"RENAMEFILESET {self.new_name}"

    class UpdateFilesetComment:
        def __init__(self, new_comment):
            self.new_comment = new_comment

        def get_new_comment(self):
            return self.new_comment

        def __eq__(self, other):
            if not isinstance(other, FilesetChange.UpdateFilesetComment):
                return False
            return self.new_comment == other.new_comment

        def __hash__(self):
            return hash(self.new_comment)

        def __str__(self):
            return f"UPDATEFILESETCOMMENT {self.new_comment}"

    class SetProperty:
        def __init__(self, property, value):
            self.property = property
            self.value = value

        def get_property(self):
            return self.property

        def get_value(self):
            return self.value

        def __eq__(self, other):
            if not isinstance(other, FilesetChange.SetProperty):
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
            if not isinstance(other, FilesetChange.RemoveProperty):
                return False
            return self.property == other.property

        def __hash__(self):
            return hash(self.property)

        def __str__(self):
            return f"REMOVEPROPERTY {self.property}"
