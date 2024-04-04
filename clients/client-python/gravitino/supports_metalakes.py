
from gravitino import MetaLake

from abc import ABC, abstractmethod
from typing import List, Dict

from gravitino.exceptions import NoSuchMetalakeException
from gravitino.meta_change import MetalakeChange
from gravitino.name_identifier import NameIdentifier


class SupportsMetalakes(ABC):

    @abstractmethod
    def list_metalakes(self) -> List[MetaLake]:
        pass

    @abstractmethod
    def load_metalake(self, ident: NameIdentifier) -> MetaLake:
        pass

    def metalake_exists(self, ident: NameIdentifier) -> bool:
        try:
            self.load_metalake(ident)
            return True
        except NoSuchMetalakeException:
            return False

    @abstractmethod
    def create_metalake(self, ident: NameIdentifier, comment: str, properties: Dict[str, str]) -> MetaLake:
        pass

    @abstractmethod
    def alter_metalake(self, ident: NameIdentifier, *changes: MetalakeChange) -> MetaLake:
        pass

    @abstractmethod
    def drop_metalake(self, ident: NameIdentifier) -> bool:
        pass