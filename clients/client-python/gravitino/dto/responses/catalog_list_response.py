from dataclasses import dataclass
from typing import List
from .base_response import BaseResponse
from ..catalog_dto import CatalogDTO


@dataclass
class CatalogListResponse(BaseResponse):
    """Represents a response for a list of catalogs with their information."""
    catalogs: List[CatalogDTO]

    def __init__(self, catalogs: List[CatalogDTO]):
        super().__init__(0)
        self.catalogs = catalogs
