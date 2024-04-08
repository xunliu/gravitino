"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from abc import ABC, abstractmethod
from datetime import datetime


class Audit(ABC):
    """Represents the audit information of an entity."""

    @abstractmethod
    def creator(self) -> str:
        """The creator of the entity.

        Return:
             the creator of the entity.
        """
        pass

    @abstractmethod
    def create_time(self) -> datetime:
        """The creation time of the entity.

        Return:
             The creation time of the entity.
        """
        pass

    @abstractmethod
    def last_modifier(self) -> str:
        """
        Return:
             The last modifier of the entity.
        """
        pass

    @abstractmethod
    def last_modified_time(self) -> datetime:
        """
        Return:
             The last modified time of the entity.
        """
        pass
