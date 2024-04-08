from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Dict

from gravitino.api.auditable import Auditable


class Fileset(Auditable):
    class Type(Enum):
        MANAGED = "managed"
        """Fileset is managed by Gravitino. 
        When specified, the data will be deleted when the fileset object is deleted"""

        EXTERNAL = "external"
        """Fileset is not managed by Gravitino. 
        When specified, the data will not be deleted when the fileset object is deleted"""

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        pass

    @abstractmethod
    def type(self) -> Type:
        pass

    @abstractmethod
    def storage_location(self) -> str:
        pass

    @abstractmethod
    def properties(self) -> Dict[str, str]:
        pass
