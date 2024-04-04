"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from gravitino.audit import Audit
from abc import ABC, abstractmethod

"""
An auditable entity is an entity that has audit information associated with it. This audit
information is used to track changes to the entity.
"""

class Auditable(ABC):

    @abstractmethod
    def audit_info(self) -> Audit:
        pass