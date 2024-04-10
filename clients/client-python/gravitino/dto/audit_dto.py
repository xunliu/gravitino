"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.audit import Audit


@dataclass
class AuditDTO(Audit, DataClassJsonMixin):
    """Data transfer object representing audit information."""

    _creator: Optional[str] = field(default=None, metadata=config(field_name='creator'))
    """The creator of the audit."""

    _create_time: Optional[str] = field(default=None, metadata=config(
        field_name='createTime'))  # TODO: Can't deserialized datetime from JSON
    """The create time of the audit."""

    _last_modifier: Optional[str] = field(default=None, metadata=config(field_name='lastModifier'))
    """The last modifier of the audit."""

    _last_modified_time: Optional[str] = field(default=None, metadata=config(
        field_name='lastModifiedTime'))  # TODO: Can't deserialized datetime from JSON
    """The last modified time of the audit."""

    # def __init__(self, creator: str = None, create_time: str = None, last_modifier: str = None,
    #              last_modified_time: str = None):
    #     self._creator: str = creator
    #     self._create_time: str = create_time
    #     self._last_modifier: str = last_modifier
    #     self._last_modified_time: str = last_modified_time

    def creator(self) -> str:
        """The creator of the entity.

        Return:
             the creator of the entity.
        """
        return self._creator

    def create_time(self) -> str:
        """The creation time of the entity.

        Return:
             The creation time of the entity.
        """
        return self._create_time

    def last_modifier(self) -> str:
        """
        Return:
             The last modifier of the entity.
        """
        return self._last_modifier

    def last_modified_time(self) -> str:
        """
        Return:
             The last modified time of the entity.
        """
        return self._last_modified_time
