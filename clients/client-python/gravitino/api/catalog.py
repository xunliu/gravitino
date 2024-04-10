from abc import abstractmethod
from enum import Enum
from typing import Dict, Optional

from gravitino.api.auditable import Auditable
from gravitino.api.supports_schemas import SupportsSchemas


class Catalog(Auditable):
    """The interface of a catalog. The catalog is the second level entity in the gravitino system,
    containing a set of tables.
    """
    class Type(Enum):
        """The type of the catalog."""

        RELATIONAL = "relational"
        """"Catalog Type for Relational Data Structure, like db.table, catalog.db.table."""

        FILESET = "fileset"
        """Catalog Type for Fileset System (including HDFS, S3, etc.), like path/to/file"""

        MESSAGING = "messaging"
        """Catalog Type for Message Queue, like kafka://topic"""

        UNSUPPORTED = "unsupported"
        """Catalog Type for test only."""

    PROPERTY_PACKAGE = "package"
    """A reserved property to specify the package location of the catalog. The "package" is a string
    of path to the folder where all the catalog related dependencies is located. The dependencies
    under the "package" will be loaded by Gravitino to create the catalog.
    
    The property "package" is not needed if the catalog is a built-in one, Gravitino will search
    the proper location using "provider" to load the dependencies. Only when the folder is in
    different location, the "package" property is needed.
    """

    @abstractmethod
    def name(self) -> str:
        """
        Return:
            The name of the catalog.
        """
        pass

    @abstractmethod
    def type(self) -> Type:
        """
        Return:
            The type of the catalog.
        """
        pass

    @abstractmethod
    def provider(self) -> str:
        """
        Return:
            The provider of the catalog.
        """
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        """The comment of the catalog. Note. this method will return null if the comment is not set for
        this catalog.

        Return:
            The provider of the catalog.
        """
        pass

    @abstractmethod
    def properties(self) -> Optional[Dict[str, str]]:
        """
        The properties of the catalog. Note, this method will return null if the properties are not set.

        Return:
            The properties of the catalog.
        """
        pass

    def as_schemas(self) -> SupportsSchemas:
        """Return the {@link SupportsSchemas} if the catalog supports schema operations.

        Raise:
            UnsupportedOperationException if the catalog does not support schema operations.

        Return:
            The {@link SupportsSchemas} if the catalog supports schema operations.
        """
        raise UnsupportedOperationException("Catalog does not support schema operations")

    def as_table_catalog(self) -> 'TableCatalog':
        """
        Raise:
            UnsupportedOperationException if the catalog does not support table operations.

        Return:
            the {@link TableCatalog} if the catalog supports table operations.
        """
        raise UnsupportedOperationException("Catalog does not support table operations")

    def as_fileset_catalog(self) -> 'FilesetCatalog':
        """
        Raise:
            UnsupportedOperationException if the catalog does not support fileset operations.

        Return:
            the {@link FilesetCatalog} if the catalog supports fileset operations.
        """
        raise UnsupportedOperationException("Catalog does not support fileset operations")

    def as_topic_catalog(self) -> 'TopicCatalog':
        """
        Return:
            the {@link TopicCatalog} if the catalog supports topic operations.
        Raise:
            UnsupportedOperationException if the catalog does not support topic operations.
        """
        raise UnsupportedOperationException("Catalog does not support topic operations")


class UnsupportedOperationException(Exception):
    pass