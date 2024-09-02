"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.api.rel.types.type import Type
from gravitino.api.rel.expressions.expression import Expression


@dataclass
class Column(ABC):
    """An interface representing a column of a {@link Table}. It defines basic properties of a column,
    such as name and data type.
    Catalog implementation needs to implement it. They should consume it in APIs like
    TableCatalog#createTable(NameIdentifier, Column[], String, Map), and report it in
    Table#columns() a default value and a generation expression.
    """

    DEFAULT_VALUE_NOT_SET = lambda: Expression.EMPTY_EXPRESSION
    """
    A default value that indicates the default value is not set. This is used in #defaultValue().
    Expression DEFAULT_VALUE_NOT_SET = () -> Expression.EMPTY_EXPRESSION;
    """

    """
    A default value that indicates the default value will be set to the current timestamp. This is
    used in defaultValue().
    Expression DEFAULT_VALUE_OF_CURRENT_TIMESTAMP = FunctionExpression.of("current_timestamp");
    """

    @abstractmethod
    def name(self) -> str:
        """
        Returns:
            The name of this column.
        """
        pass

    @abstractmethod
    def data_type(self) -> Type:
        """
        Returns:
            The data type of this column.
        """
        pass

    @abstractmethod
    def comment(self) -> str:
        """
        Returns:
            The comment of this column, null if not specified.
        """
        pass

    @abstractmethod
    def nullable(self) -> bool:
        """
        Returns:
            True if this column may produce null values. Default is true.
        """
        pass

    @abstractmethod
    def auto_increment(self) -> bool:
        """
        Returns:
            True if this column is an auto-increment column. Default is false.
        """
        pass

    @abstractmethod
    def default_value(self) -> Expression:
        """
        Returns:
            The default value of this column, {@link Column#DEFAULT_VALUE_NOT_SET} if not specified.
        """
        pass

    @staticmethod
    def of(name: str, data_type: type, comment: str = None, nullable=True, auto_increment=False,
           default_value: Expression = None) -> 'ColumnImpl':
        """
        Create a Column instance.

        Args:
            name: The name of the column.
            data_type: The data type of the column.
            comment: The comment of the column.
            nullable: The if this column may produce null values.
            auto_increment: The default value of the column. {@link Column#DEFAULT_VALUE_NOT_SET} if null.
            default_value: The name of the column.

        Returns:
            The Column instance.
        """
        return ColumnImpl(name, data_type, comment, nullable, auto_increment,
                          default_value if default_value is not None else Column.DEFAULT_VALUE_NOT_SET)


class ColumnImpl(Column):
    """
    An interface representing a column of a Table. It defines basic properties of a column,
    such as name and data type.
    """
    _name: str = field(metadata=config(field_name="name"))
    """The name of the column."""

    _data_type: Type = field(metadata=config(field_name="dataType"))
    """The data type of the column."""

    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    """The comment associated with the column."""

    _nullable: bool = field(metadata=config(field_name="nullable"), default=True)
    """Whether the column value can be null."""

    _auto_increment: bool = field(metadata=config(field_name="autoIncrement"), default=False)
    """Whether the column is an auto-increment column."""

    _default_value: Optional[Expression] = field(
        metadata=config(field_name="defaultValue"),
        default_factory=lambda: Expression.EMPTY_EXPRESSION,
    )
    """The default value of the column."""

    def __init__(self, name: str, data_type: type, comment: str, nullable: bool, auto_increment: bool,
                 default_value: Expression):
        assert name, "Column name cannot be null"
        assert data_type is not None, "Column data type cannot be null"
        self.name = name
        self.data_type = data_type
        self.comment = comment
        self.nullable = nullable
        self.auto_increment = auto_increment
        self.default_value = default_value

    def name(self) -> str:
        return self._name

    def data_type(self) -> Type:
        return self._data_type

    def comment(self) -> str:
        return self._comment

    def nullable(self) -> bool:
        return self._nullable

    def auto_increment(self) -> bool:
        return self._auto_increment

    def default_value(self) -> Expression:
        return self._default_value


def __eq__(self, other):
    if self is other:
        return True
    if not isinstance(other, ColumnImpl):
        return False
    return (self.nullable == other.nullable and
            self._auto_increment == other.auto_increment and
            self._name == other.name and
            self._data_type == other.data_type and
            self._comment == other.comment and
            self._default_value == other.default_value)


def __hash__(self):
    return hash((self._name, self._data_type, self._comment, self._nullable, self._auto_increment, self._default_value))
