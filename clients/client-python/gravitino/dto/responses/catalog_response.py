from dataclasses import dataclass

from .base_response import BaseResponse
from ..catalog_dto import CatalogDTO


@dataclass
class CatalogResponse(BaseResponse):
    catalog: CatalogDTO = None

    # def __init__(self, catalog: CatalogDTO):
    #     super().__init__(0)
    #     self.catalog = catalog

    def validate(self):
        super().validate()

        assert self.catalog is not None, "catalog must not be null"
        assert self.catalog.name is not None, "catalog 'name' must not be null and empty"
        assert self.catalog.type is not None, "catalog 'type' must not be null"
        assert self.catalog.audit is not None, "catalog 'audit' must not be null"
