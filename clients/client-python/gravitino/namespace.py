"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import string

from gravitino.exceptions.illegal_namespace_exception import IllegalNamespaceException
from typing import List


class Namespace:
    """
     * A namespace is a sequence of levels separated by dots. It's used to identify a metalake, a
     * catalog or a schema. For example, "metalake1", "metalake1.catalog1" and
     * "metalake1.catalog1.schema1" are all valid namespaces.
    """

    EMPTY = None
    DOT = "."

    def __init__(self, levels: List[str]):
        self.levels = levels

    @staticmethod
    def empty() -> 'Namespace':
        return Namespace([])

    @staticmethod
    def of(*levels: str) -> 'Namespace':
        if levels is None:
            raise ValueError("Cannot create a namespace with null levels")
        if len(levels) == 0:
            return Namespace.empty()

        for level in levels:
            if level is None or level == "":
                raise ValueError("Cannot create a namespace with null or empty level")

        return Namespace(list(levels))

    @staticmethod
    def of_metalake() -> 'Namespace':
        return Namespace.empty()

    @staticmethod
    def of_catalog(metalake: str) -> 'Namespace':
        return Namespace.of(metalake)

    @staticmethod
    def of_schema(metalake: str, catalog: str) -> 'Namespace':
        return Namespace.of(metalake, catalog)

    @staticmethod
    def of_table(metalake: str, catalog: str, schema: str) -> 'Namespace':
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def of_fileset(metalake: str, catalog: str, schema: str) -> 'Namespace':
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def of_topic(metalake: str, catalog: str, schema: str) -> 'Namespace':
        return Namespace.of(metalake, catalog, schema)

    @staticmethod
    def check_metalake(namespace: 'Namespace') -> None:
        if not namespace or not namespace.is_empty():
            raise ValueError(f"Metalake namespace must be non-null and empty, the input namespace is {namespace}")

    @staticmethod
    def check_catalog(namespace: 'Namespace') -> None:
        if not namespace or namespace.length() != 1:
            raise ValueError(f"Catalog namespace must be non-null and have 1 level, the input namespace is {namespace}")

    @staticmethod
    def check_schema(namespace: 'Namespace') -> None:
        if not namespace or namespace.length() != 2:
            raise ValueError(f"Schema namespace must be non-null and have 2 levels, the input namespace is {namespace}")

    @staticmethod
    def check_table(namespace: 'Namespace') -> None:
        if not namespace or namespace.length() != 3:
            raise ValueError(f"Table namespace must be non-null and have 3 levels, the input namespace is {namespace}")

    @staticmethod
    def check_fileset(namespace: 'Namespace') -> None:
        if not namespace or namespace.length() != 3:
            raise ValueError(
                f"Fileset namespace must be non-null and have 3 levels, the input namespace is {namespace}")

    @staticmethod
    def check_topic(namespace: 'Namespace') -> None:
        if not namespace or namespace.length() != 3:
            raise ValueError(f"Topic namespace must be non-null and have 3 levels, the input namespace is {namespace}")

    def levels(self) -> List[str]:
        return self.levels

    def level(self, pos: int) -> str:
        if pos < 0 or pos >= len(self.levels):
            raise ValueError("Invalid level position")
        return self.levels[pos]

    def length(self) -> int:
        return len(self.levels)

    def is_empty(self) -> bool:
        return len(self.levels) == 0

    def __eq__(self, other: 'Namespace') -> bool:
        if not isinstance(other, Namespace):
            return False
        return self.levels == other.levels

    def __hash__(self) -> int:
        return hash(tuple(self.levels))

    def __str__(self) -> str:
        return Namespace.DOT.join(self.levels)

    @staticmethod
    def check(expression: bool, message: str, *args) -> None:
        if not expression:
            raise ValueError(message.format(*args))
