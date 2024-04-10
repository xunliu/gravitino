from abc import ABC, abstractmethod
from typing import Dict
from dataclasses import dataclass

from dataclasses_json import DataClassJsonMixin

from .audit_dto import AuditDTO
from ..api.catalog import Catalog


@dataclass
class CatalogDTO(Catalog, DataClassJsonMixin):
    """Data transfer object representing catalog information."""

    name: str
    type: Catalog.Type
    provider: str
    comment: str
    properties: Dict[str, str]
    audit: AuditDTO = None

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
