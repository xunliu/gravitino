from abc import ABC, abstractmethod
from typing import List, Dict

from gravitino.catalog import Catalog
from gravitino.catalog_change import CatalogChange
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class SupportsCatalogs(ABC):

    @abstractmethod
    def listCatalogs(self, namespace: Namespace) -> List[NameIdentifier]:
        pass

    @abstractmethod
    def listCatalogsInfo(self, namespace: Namespace) -> List[Catalog]:
        pass

    @abstractmethod
    def loadCatalog(self, ident: NameIdentifier) -> Catalog:
        pass

    @abstractmethod
    def catalogExists(self, ident: NameIdentifier) -> bool:
        pass

    @abstractmethod
    def createCatalog(self, ident: NameIdentifier, type: Catalog.Type, provider: str, comment: str, properties: Dict[str, str]) -> Catalog:
        pass

    @abstractmethod
    def alterCatalog(self, ident: NameIdentifier, *changes: CatalogChange) -> Catalog:
        pass

    @abstractmethod
    def dropCatalog(self, ident: NameIdentifier) -> bool:
        pass
