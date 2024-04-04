from abc import ABC, abstractmethod
from typing import Dict
from dataclasses import dataclass

class CatalogChange(ABC):

    @staticmethod
    @abstractmethod
    def rename(new_name: str) -> 'CatalogChange':
        pass

    @staticmethod
    @abstractmethod
    def update_comment(new_comment: str) -> 'CatalogChange':
        pass

    @staticmethod
    @abstractmethod
    def set_property(property: str, value: str) -> 'CatalogChange':
        pass

    @staticmethod
    @abstractmethod
    def remove_property(property: str) -> 'CatalogChange':
        pass

@dataclass
class RenameCatalog(CatalogChange):
    new_name: str

@dataclass
class UpdateCatalogComment(CatalogChange):
    new_comment: str

@dataclass
class SetProperty(CatalogChange):
    property: str
    value: str

@dataclass
class RemoveProperty(CatalogChange):
    property: str
