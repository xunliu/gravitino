from abc import abstractmethod
from enum import Enum
from typing import Dict, Optional

from gravitino.api.auditable import Auditable
from gravitino.api.supports_schemas import SupportsSchemas


class Catalog(Auditable):
    class Type(Enum):
        RELATIONAL = "relational"
        """"Catalog Type for Relational Data Structure, like db.table, catalog.db.table."""

        FILESET = "fileset"
        """Catalog Type for Fileset System (including HDFS, S3, etc.), like path/to/file"""

        MESSAGING = "messaging"
        """Catalog Type for Message Queue, like kafka://topic"""

        UNSUPPORTED = "unsupported"
        """Catalog Type for test only."""

    PROPERTY_PACKAGE = "package"

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def type(self) -> Type:
        pass

    @abstractmethod
    def provider(self) -> str:
        pass

    @abstractmethod
    def comment(self) -> Optional[str]:
        pass

    @abstractmethod
    def properties(self) -> Optional[Dict[str, str]]:
        pass

    def as_schemas(self) -> SupportsSchemas:
        raise UnsupportedOperationException("Catalog does not support schema operations")

    # def as_table_catalog(self) -> TableCatalog:
    #     raise UnsupportedOperationException("Catalog does not support table operations")

    def as_fileset_catalog(self) -> 'FilesetCatalog':
        pass
    # raise UnsupportedOperationException("Catalog does not support fileset operations")

    # def as_topic_catalog(self) -> TopicCatalog:
    #     raise UnsupportedOperationException("Catalog does not support topic operations")


class UnsupportedOperationException(Exception):
    pass