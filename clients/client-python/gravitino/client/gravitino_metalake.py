from typing import List, Dict
from datetime import datetime
from abc import ABC, abstractmethod

from gravitino.catalog import Catalog
from gravitino.catalog_change import CatalogChange
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.supports_catalogs import SupportsCatalogs


class GravitinoMetalake(MetalakeDTO): # SupportsCatalogs

    def __init__(self, name: str, comment: str, properties: Dict[str, str], auditDTO: AuditDTO, restClient):
        super().__init__(name, comment, properties, auditDTO)
        self.restClient = restClient

    # def listCatalogs(self, namespace: Namespace) -> List[NameIdentifier]:
    #     return []
    #
    # def listCatalogsInfo(self, namespace: Namespace) -> List[Catalog]:
    #     return []
    #
    # def loadCatalog(self, ident: NameIdentifier) -> Catalog:
    #     pass
    #
    # def createCatalog(self, ident: NameIdentifier, type: Catalog.Type, provider: str, comment: str, properties: Dict[str, str]) -> Catalog:
    #     print()
    #
    # def alterCatalog(self, ident: NameIdentifier, *changes: CatalogChange) -> Catalog:
    #     print()
    #
    # def dropCatalog(self, ident: NameIdentifier) -> bool:
    #     return True

    class Builder(MetalakeDTO.Builder):

        def __init__(self):
            super().__init__()
            self.restClient = None

        def with_rest_client(self, restClient):
            self.restClient = restClient
            return self

        def build(self) -> 'GravitinoMetalake':
            if self.restClient is None:
                raise ValueError("restClient must be set")
            if not self.name:
                raise ValueError("name must not be null or empty")
            if self.audit is None:
                raise ValueError("audit must not be null")

            return GravitinoMetalake(self.name, self.comment, self.properties, self.audit, self.restClient)

    @staticmethod
    def builder() -> Builder:
        return GravitinoMetalake.Builder()
