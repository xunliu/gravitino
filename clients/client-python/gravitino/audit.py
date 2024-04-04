"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from abc import ABC, abstractmethod
from datetime import datetime

"""
Represents the audit information of an entity.
"""
class Audit(ABC):
    """
    The creator of the entity.
    @return the creator of the entity.
    """
    @abstractmethod
    def creator(self) -> str:
        pass

    """
    The creation time of the entity.
    @return The creation time of the entity.
    """
    @abstractmethod
    def create_time(self) -> datetime:
        pass

    """
    @return The last modifier of the entity.
    """
    @abstractmethod
    def last_modifier(self) -> str:
        pass

    """
    @return The last modified time of the entity.
    """
    @abstractmethod
    def last_modified_time(self) -> datetime:
        pass
