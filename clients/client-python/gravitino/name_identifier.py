from gravitino.exceptions.illegal_name_identifier_exception import IllegalNameIdentifierException
from gravitino.namespace import Namespace


class NameIdentifier:
    """
     * A name identifier is a sequence of names separated by dots. It's used to identify a metalake, a
     * catalog, a schema or a table. For example, "metalake1" can represent a metalake,
     * "metalake1.catalog1" can represent a catalog, "metalake1.catalog1.schema1" can represent a
     * schema.
    """

    DOT = '.'

    def __init__(self, namespace: Namespace, name: str):
        self.__namespace = namespace
        self.__name = name

    def NameIdentifier(self, namespace, name):
        self.check(namespace is not None, "Cannot create a NameIdentifier with null namespace")
        self.check(name is not None and name != "", "Cannot create a NameIdentifier with null or empty name")

        self.__namespace = namespace
        self.__name = name

    @staticmethod
    def of(*names: str) -> 'NameIdentifier':
        NameIdentifier.check(names is None, "Cannot create a NameIdentifier with null names")
        NameIdentifier.check(len(names) == 0, "Cannot create a NameIdentifier with no names")

        return NameIdentifier(Namespace.of(names[:-1]), names[-1])

    @staticmethod
    def of_namespace(namespace: Namespace, name: str) -> 'NameIdentifier':
        return NameIdentifier(namespace, name)

    @staticmethod
    def of_metalake(metalake: str) -> 'NameIdentifier':
        return NameIdentifier.of(metalake)

    @staticmethod
    def of_catalog(metalake: str, catalog: str) -> 'NameIdentifier':
        return NameIdentifier.of(metalake, catalog)

    @staticmethod
    def of_schema(metalake: str, catalog: str, schema: str) -> 'NameIdentifier':
        return NameIdentifier.of(metalake, catalog, schema)

    @staticmethod
    def of_table(metalake: str, catalog: str, schema: str, table: str) -> 'NameIdentifier':
        return NameIdentifier.of(metalake, catalog, schema, table)

    @staticmethod
    def of_fileset(metalake: str, catalog: str, schema: str, fileset: str) -> 'NameIdentifier':
        return NameIdentifier.of(metalake, catalog, schema, fileset)

    @staticmethod
    def of_topic(metalake: str, catalog: str, schema: str, topic: str) -> 'NameIdentifier':
        return NameIdentifier.of(metalake, catalog, schema, topic)

    @staticmethod
    def check_metalake(ident: 'NameIdentifier') -> None:
        NameIdentifier.check(ident is None, "Metalake identifier must not be null")
        Namespace.check_metalake(ident.__namespace)

    @staticmethod
    def check_catalog(ident: 'NameIdentifier') -> None:
        NameIdentifier.check(ident is None, "Catalog identifier must not be null")
        Namespace.check_catalog(ident.__namespace)

    @staticmethod
    def check_schema(ident: 'NameIdentifier') -> None:
        NameIdentifier.check(ident is None, "Schema identifier must not be null")
        Namespace.check_schema(ident.__namespace)

    @staticmethod
    def check_table(ident: 'NameIdentifier') -> None:
        NameIdentifier.check(ident is None, "Table identifier must not be null")
        Namespace.check_table(ident.__namespace)

    @staticmethod
    def check_fileset(ident: 'NameIdentifier') -> None:
        NameIdentifier.check(ident is None, "Fileset identifier must not be null")
        Namespace.check_fileset(ident.__namespace)

    @staticmethod
    def check_topic(ident: 'NameIdentifier') -> None:
        NameIdentifier.check(ident is None, "Topic identifier must not be null")
        Namespace.check_topic(ident.__namespace)

    @staticmethod
    def parse(identifier: str) -> 'NameIdentifier':
        NameIdentifier.check(identifier is None or identifier == '', "Cannot parse a null or empty identifier")

        parts = identifier.split(NameIdentifier.DOT)
        return NameIdentifier.of(*parts)

    def has_namespace(self):
        return not self.__namespace.is_empty()

    def get_namespace(self):
        return self.__namespace

    def get_name(self):
        return self.__name

    def __eq__(self, other):
        if not isinstance(other, NameIdentifier):
            return False
        return self.__namespace == other.__namespace and self.__name == other.__name

    def __hash__(self):
        return hash((self.__namespace, self.__name))

    def __str__(self):
        if self.has_namespace():
            return str(self.__namespace) + "." + self.__name
        else:
            return self.__name

    @staticmethod
    def check(condition, message, *args):
        if not condition:
            raise IllegalNameIdentifierException(message.format(*args))
