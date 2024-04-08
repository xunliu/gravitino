from abc import ABC, abstractmethod
from typing import List, Dict, Optional

from gravitino.api.schema import Schema
from gravitino.api.schema_change import SchemaChange
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


class NoSuchSchemaException(Exception):
    """Exception raised if the schema does not exist."""
    pass


class SupportsSchemas(ABC):
    """
    The Catalog interface to support schema operations. If the implemented catalog has schema
    semantics, it should implement this interface.
    """

    @abstractmethod
    def list_schemas(self, namespace: Namespace) -> List[NameIdentifier]:
        """
        List schemas under a namespace.

        If an entity such as a table, view exists, its parent schemas must also exist and must be
        returned by this discovery method. For example, if table a.b.t exists, this method invoked as
        list_schemas(a) must return [a.b] in the result array.

        :param namespace: The namespace to list.
        :return: A list of schema identifiers under the namespace.
        :raises NoSuchCatalogException: If the catalog does not exist.
        """
        pass

    def schema_exists(self, ident: NameIdentifier) -> bool:
        """
        Check if a schema exists.

        If an entity such as a table, view exists, its parent namespaces must also exist. For
        example, if table a.b.t exists, this method invoked as schema_exists(a.b) must return true.

        :param ident: The name identifier of the schema.
        :return: True if the schema exists, false otherwise.
        """
        try:
            self.load_schema(ident)
            return True
        except NoSuchSchemaException:
            return False

    @abstractmethod
    def create_schema(self, ident: NameIdentifier, comment: Optional[str], properties: Dict[str, str]) -> Schema:
        """
        Create a schema in the catalog.

        :param ident: The name identifier of the schema.
        :param comment: The comment of the schema.
        :param properties: The properties of the schema.
        :return: The created schema.
        :raises NoSuchCatalogException: If the catalog does not exist.
        :raises SchemaAlreadyExistsException: If the schema already exists.
        """
        pass

    @abstractmethod
    def load_schema(self, ident: NameIdentifier) -> Schema:
        """
        Load metadata properties for a schema.

        :param ident: The name identifier of the schema.
        :return: A schema.
        :raises NoSuchSchemaException: If the schema does not exist (optional).
        """
        pass

    @abstractmethod
    def alter_schema(self, ident: NameIdentifier, changes: List[SchemaChange]) -> Schema:
        """
        Apply the metadata change to a schema in the catalog.

        :param ident: The name identifier of the schema.
        :param changes: The metadata changes to apply.
        :return: The altered schema.
        :raises NoSuchSchemaException: If the schema does not exist.
        """
        pass

    @abstractmethod
    def drop_schema(self, ident: NameIdentifier, cascade: bool) -> bool:
        """
        Drop a schema from the catalog. If cascade option is true, recursively drop all objects within
        the schema.

        Args:
            ident: The name identifier of the schema.
            cascade: If true, recursively drop all objects within the schema.

        Return:
            True if the schema exists and is dropped successfully, false otherwise.

        Raises:
            NonEmptySchemaException: If the schema is not empty and cascade is false.
        """
        pass
