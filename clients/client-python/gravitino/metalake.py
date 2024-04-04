"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from abc import abstractmethod

from gravitino.auditable import Auditable


class Metalake(Auditable):
    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def comment(self):
        pass

    @property
    @abstractmethod
    def properties(self):
        pass
