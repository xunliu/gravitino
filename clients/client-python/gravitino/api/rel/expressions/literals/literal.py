from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List, Type

from gravitino.api.rel.expressions.expression import Expression

T = TypeVar('T')


class Literal(Expression, Generic[T], ABC):
    @abstractmethod
    def value(self) -> T:
        pass

    @abstractmethod
    def data_type(self) -> Type:
        pass

    def children(self) -> List[Expression]:
        return Expression.EMPTY_EXPRESSION
