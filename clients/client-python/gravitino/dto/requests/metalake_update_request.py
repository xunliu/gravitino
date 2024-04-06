from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.meta_change import MetalakeChange
from gravitino.rest.rest_request import RESTRequest

@dataclass
class MetalakeUpdateRequest222(RESTRequest):
    type : str = field(metadata=config(field_name='@type')) #config(field_name='@type')


class MetalakeUpdateRequest:

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def metalakeChange(self):
        pass

    @dataclass
    class RenameMetalakeRequest(MetalakeUpdateRequest222):
        newName: str = None

        def __init__(self, newName: str):
            self.newName = newName
            self.type = "rename"

        # def __post_init__(self):
        #     self.type = "rename"

        def validate(self):
            if not self.newName:
                raise ValueError('"newName" field is required and cannot be empty')

        def metalakeChange(self):
            return MetalakeChange.rename(self.newName)

    @dataclass
    class UpdateMetalakeCommentRequest(MetalakeUpdateRequest222):
        newComment: str = None

        def __init__(self, newComment: str):
            self.newComment = newComment
            self.type = "updateComment"

        # def __post_init__(self):
        #     self.type = "updateComment"

        def validate(self):
            if not self.newComment:
                raise ValueError('"newComment" field is required and cannot be empty')

        def metalakeChange(self):
            return MetalakeChange.update_comment(self.newComment)

    @dataclass
    class SetMetalakePropertyRequest(MetalakeUpdateRequest222):
        property: str = None
        value: str = None

        def __init__(self, property: str, value: str):
            self.property = property
            self.value = value
            self.type = "setProperty"


        # def __post_init__(self):
        #     self.type = "setProperty"

        def validate(self):
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')
            if not self.value:
                raise ValueError('"value" field is required and cannot be empty')

        def metalakeChange(self):
            return MetalakeChange.setProperty(self.property, self.value)

    @dataclass
    class RemoveMetalakePropertyRequest(MetalakeUpdateRequest222):
        property: str = None

        # def __post_init__(self):
        #     self.type = "removeProperty"

        def __init__(self, property: str):
            self.property = property
            self.type = "removeProperty"

        def validate(self):
            if not self.property:
                raise ValueError('"property" field is required and cannot be empty')

        def metalakeChange(self):
            return MetalakeChange.removeProperty(self.property)