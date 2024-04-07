"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, config


@dataclass
class AuditDTO(DataClassJsonMixin):
    """
    Data transfer object representing audit information.
    """
    creator: str
    create_time: str = field(metadata=config(field_name='createTime'))  # TODO: Can't deserialized datetime from JSON
    last_modifier: str = field(metadata=config(field_name='lastModifier'))
    last_modified_time: str = field(metadata=config(field_name='lastModifiedTime'))  # TODO: Can't deserialized datetime from JSON

    def __init__(self, creator: str = None, create_time: str = None, last_modifier: str = None, last_modified_time: str = None):
        self.creator: str = creator
        self.create_time: str = create_time
        self.last_modifier: str = last_modifier
        self.last_modified_time: str = last_modified_time

    # def creator(self):
    #     """
    #     The creator of the audit.
    #     """
    #     return self.creator
    #
    # def create_time(self):
    #     """
    #     The create time of the audit.
    #     """
    #     return self.create_time
    #
    # def last_modifier(self):
    #     """
    #     The last modifier of the audit.
    #     """
    #     return self.last_modifier
    #
    # def last_modified_time(self):
    #     """
    #     The last modified time of the audit.
    #     """
    #     return self.last_modified_time
    #
    # @classmethod
    # def builder(cls):
    #     """
    #     Creates a new Builder for constructing an Audit DTO.
    #     Return:
    #          A new Builder instance.
    #     """
    #     return cls.Builder()

    # class Builder:
    #     """
    #     Builder class for constructing an AuditDTO instance.
    #     Args:
    #         The type of the builder instance.
    #     """
    #     def __init__(self):
    #         self.creator = None
    #         self.create_time = None
    #         self.last_modifier = None
    #         self.last_modified_time = None
    #
    #     def with_creator(self, creator):
    #         """
    #         Sets the creator for the audit.
    #         Args:
    #            creator: The creator of the audit.
    #         Return:
    #            The builder instance.
    #         """
    #         self.creator = creator
    #         return self
    #
    #     def with_create_time(self, create_time):
    #         """
    #         Sets the create time for the audit.
    #         Args:
    #           createTime: The create time of the audit.
    #         Return:
    #            The builder instance.
    #         """
    #         self.create_time = create_time
    #         return self
    #
    #     def with_last_modifier(self, last_modifier):
    #         """
    #         Sets who last modified the audit.
    #         Args:
    #              lastModifier: The last modifier of the audit.
    #         Return：
    #             The builder instance.
    #         """
    #         self.last_modifier = last_modifier
    #         return self
    #
    #     def with_last_modified_time(self, last_modified_time):
    #         """
    #         Sets the last modified time for the audit.
    #         Args:
    #             lastModifiedTime: The last modified time of the audit.
    #         Return:
    #              The builder instance.
    #         """
    #         self.last_modified_time = last_modified_time
    #         return self
    #
    #     def build(self):
    #         """
    #         Builds an instance of AuditDTO using the builder's properties.
    #         Return:
    #            An instance of AuditDTO.
    #         """
    #         return AuditDTO(self.creator, self.create_time, self.last_modifier, self.last_modified_time)
