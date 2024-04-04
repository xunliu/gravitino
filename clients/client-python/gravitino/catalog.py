from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict

from gravitino.auditable import Auditable


class Catalog(Auditable, ABC):

    class Type(Enum):
        RELATIONAL = 1
        FILESET = 2
        MESSAGING = 3
        UNSUPPORTED = 4

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
    def comment(self) -> str:
        pass

    @abstractmethod
    def properties(self) -> Dict[str, str]:
        pass

    # @abstractmethod
    # def asSchemas(self) -> SupportsSchemas:
    #     pass
    #
    # @abstractmethod
    # def asTableCatalog(self) -> TableCatalog:
    #     pass
    #
    # @abstractmethod
    # def asFilesetCatalog(self) -> FilesetCatalog:
    #     pass
    #
    # @abstractmethod
    # def asTopicCatalog(self) -> TopicCatalog:
    #     pass
