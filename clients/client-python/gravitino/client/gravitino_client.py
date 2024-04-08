from typing import List, Dict

from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class NoSuchMetalakeException(Exception):
    pass


class NoSuchCatalogException(Exception):
    pass


class CatalogAlreadyExistsException(Exception):
    pass


class GravitinoClient(GravitinoClientBase):
    metalake: GravitinoMetalake

    def __init__(self, uri: str, metalake_name: str):
        super().__init__(uri)
        self.metalake = super().load_metalake(NameIdentifier.of(metalake_name))

    def get_metalake(self) -> GravitinoMetalake:
        return self.metalake

    def list_catalogs(self, namespace: Namespace) -> List[NameIdentifier]:
        return self.get_metalake().list_catalogs(namespace)

    def list_catalogs_info(self, namespace: Namespace) -> List[Catalog]:
        return self.get_metalake().list_catalogs_info(namespace)

    def load_catalog(self, ident: NameIdentifier) -> Catalog:
        return self.get_metalake().load_catalog(ident)

    def create_catalog(self, ident: NameIdentifier, type: Catalog.Type, provider: str, comment: str, properties: Dict[str, str]) -> Catalog:
        return self.get_metalake().create_catalog(ident, type, provider, comment, properties)

    def alter_catalog(self, ident: NameIdentifier, *changes: CatalogChange):
        return self.get_metalake().alter_catalog(ident, *changes)

    def drop_catalog(self, ident: NameIdentifier):
        return self.get_metalake().drop_catalog(ident)
