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
    last_modified_time: str = field(
        metadata=config(field_name='lastModifiedTime'))  # TODO: Can't deserialized datetime from JSON

    def __init__(self, creator: str = None, create_time: str = None, last_modifier: str = None,
                 last_modified_time: str = None):
        self.creator: str = creator
        self.create_time: str = create_time
        self.last_modifier: str = last_modifier
        self.last_modified_time: str = last_modified_time
