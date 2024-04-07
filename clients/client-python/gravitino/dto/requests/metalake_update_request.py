from abc import abstractmethod, ABC
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.meta_change import MetalakeChange
from gravitino.rest.rest_request import RESTRequest


@dataclass
class MetalakeUpdateRequestType(RESTRequest, ABC):
    type: str = field(metadata=config(field_name='@type'))

    def __init__(self, type: str):
        self.type = type


class MetalakeUpdateRequest:

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def metalake_change(self):
        pass

    @dataclass
    class RenameMetalakeRequest(MetalakeUpdateRequestType):
        newName: str = None

        def __init__(self, newName: str):
            super().__init__("rename")
            self.newName = newName

        def validate(self):
            if not self.newName:
                raise ValueError('"newName" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.rename(self.newName)

    @dataclass
    class UpdateMetalakeCommentRequest(MetalakeUpdateRequestType):
        newComment: str = None

        def __init__(self, newComment: str):
            super().__init__("updateComment")
            self.newComment = newComment

        def validate(self):
            if not self.newComment:
                raise ValueError('"newComment" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.update_comment(self.newComment)

    @dataclass
    class SetMetalakePropertyRequest(MetalakeUpdateRequestType):
        property: str = None
        value: str = None

        def __init__(self, property: str, value: str):
            super().__init__("setProperty")
            self.property = property
            self.value = value

        def validate(self):
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self.value:
                raise ValueError('"value" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.set_property(self.property, self.value)

    @dataclass
    class RemoveMetalakePropertyRequest(MetalakeUpdateRequestType):
        property: str = None

        def __init__(self, property: str):
            super().__init__("removeProperty")
            self.property = property

        def validate(self):
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')

        def metalake_change(self):
            return MetalakeChange.remove_property(self.property)
