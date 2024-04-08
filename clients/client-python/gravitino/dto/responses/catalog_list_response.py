from dataclasses import dataclass
from typing import List
from .base_response import BaseResponse
from ..catalog_dto import CatalogDTO


@dataclass
class CatalogListResponse(BaseResponse):
    catalogs: List[CatalogDTO]

    def __init__(self, catalogs: List[CatalogDTO]):
        super().__init__(0)
        self.catalogs = catalogs

    # def __str__(self):
    #     return f"CatalogListResponse(catalogs={self.catalogs})"
