from abc import ABC, abstractmethod
from typing import Dict
from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin

from .audit_dto import AuditDTO
# from ..api.audit import Audit
from ..api.catalog import Catalog
# from ..catalog.fileset_catalog import FilesetCatalog


@dataclass
class CatalogDTO(Catalog, DataClassJsonMixin):
    name: str
    type: Catalog.Type
    provider: str
    comment: str
    properties: Dict[str, str]
    audit: AuditDTO = None

    # def __init__(self, name: str = None,
    #              type: Catalog.Type = Catalog.Type.UNSUPPORTED,
    #              provider: str = None, comment: str = None,
    #              properties: Dict[str, str] = None,
    #              audit: AuditDTO = None):
    #     self.name = name
    #     self.type = type
    #     self.provider = provider
    #     self.comment = comment
    #     self.properties = properties
    #     self.audit = audit

    def name(self) -> str:
        return self.name

    def type(self) -> Catalog.Type:
        return self.type

    def provider(self) -> str:
        return self.provider

    def comment(self) -> str:
        return self.comment

    def properties(self) -> Dict[str, str]:
        return self.properties

    def audit_info(self) -> AuditDTO:
        return self.audit

    # def build(self):
    #     Preconditions.checkArgument(StringUtils.isNotBlank(self.name), "name cannot be null or empty")
    #     Preconditions.checkArgument(self.type is not None, "type cannot be null")
    #     Preconditions.checkArgument(StringUtils.isNotBlank(self.provider), "provider cannot be null or empty")
    #     Preconditions.checkArgument(self.audit is not None, "audit cannot be null")
    #
    #     return CatalogDTO(self.name, self.type, self.provider, self.comment, self.properties, self.audit)
