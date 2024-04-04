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


class GravitinoMetalake(MetalakeDTO, SupportsCatalogs):

    def __init__(self, name: str, comment: str, properties: Dict[str, str], auditDTO: AuditDTO, restClient):
        super().__init__(name, comment, properties, auditDTO)
        self.restClient = restClient

    def listCatalogs(self, namespace: Namespace) -> List[NameIdentifier]:
        pass

    def listCatalogsInfo(self, namespace: Namespace) -> List[Catalog]:
        pass

    def loadCatalog(self, ident: NameIdentifier) -> Catalog:
        pass

    def createCatalog(self, ident: NameIdentifier, type: Catalog.Type, provider: str, comment: str, properties: Dict[str, str]) -> Catalog:
        pass

    def alterCatalog(self, ident: NameIdentifier, *changes: CatalogChange) -> Catalog:
        pass

    def dropCatalog(self, ident: NameIdentifier) -> bool:
        pass
    #
    # class Builder(MetalakeDTO.Builder):
    #
    #     def __init__(self):
    #         super().__init__()
    #         self.restClient = None
    #
    #     def withRestClient(self, restClient):
    #         self.restClient = restClient
    #         return self
    #
    #     def build(self) -> GravitinoMetalake:
    #         if self.restClient is None:
    #             raise ValueError("restClient must be set")
    #         if not self.name:
    #             raise ValueError("name must not be null or empty")
    #         if self.audit is None:
    #             raise ValueError("audit must not be null")
    #
    #         return GravitinoMetalake(self.name, self.comment, self.properties, self.audit, self.restClient)
    #
    # @staticmethod
    # def builder() -> Builder:
    #     return GravitinoMetalake.Builder()
