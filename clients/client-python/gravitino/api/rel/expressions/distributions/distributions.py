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

from typing import List

from gravitino.api.rel.expressions.distributions.distribution import Distribution
from gravitino.api.rel.expressions.distributions.strategy import Strategy
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.named_reference import NamedReference


class DistributionImpl(Distribution):
    """
    Create a distribution on columns. Like distribute by (a) or (a, b), for complex like
    distributing by (func(a), b) or (func(a), func(b)), please use DistributionImpl.Builder
    """

    _strategy: Strategy
    _number: int
    _expressions: List[Expression]

    def __init__(self, strategy: Strategy, number: int, expressions: List[Expression]):
        self._strategy = strategy
        self._number = number
        self._expressions = expressions

    def strategy(self) -> Strategy:
        """
        Get the strategy of the distribution.

        Returns:
            The strategy of the distribution.
        """
        return self._strategy

    def number(self) -> int:
        """
        Get the number of buckets of the distribution.

        Returns:
            The number of buckets of the distribution.
        """
        return self._number

    def expressions(self) -> List[Expression]:
        """
        Get the expressions of the distribution.

        Returns:
            The expressions of the distribution.
        """
        return self._expressions

    def __str__(self) -> str:
        return f"DistributionImpl{{strategy={self._strategy}, number={self._number}, expressions={self._expressions}}}"

    def __eq__(self, other) -> bool:
        if self is other:
            return True
        if not isinstance(other, DistributionImpl):
            return False
        return (self._number == other.number and
                self._strategy == other.strategy and
                self._expressions == other.expressions)

    def __hash__(self) -> int:
        return hash((self._strategy, self._number, tuple(self._expressions)))


class Distributions:
    """Helper methods to create distributions to pass into Apache Gravitino."""

    NONE = None
    """NONE is used to indicate that there is no distribution."""

    HASH = None
    """List bucketing strategy hash, TODO: #1505 Separate the bucket number from the Distribution."""

    RANGE = None
    """List bucketing strategy range, TODO: #1505 Separate the bucket number from the Distribution."""

    @staticmethod
    def even(number: int, *expressions: Expression) -> DistributionImpl:
        """Create a distribution by evenly distributing the data across the number of buckets.

        Args:
            number: The number of buckets
            expressions: The expressions to distribute by

        Returns:
            The created even distribution
        """
        return DistributionImpl(Strategy.EVEN, number, list(expressions))

    @staticmethod
    def hash(number: int, *expressions: Expression) -> DistributionImpl:
        """Create a distribution by hashing the data across the number of buckets.

        Args:
            number: The number of buckets
            expressions: The expressions to distribute by

        Returns:
            The created even distribution
        """
        return DistributionImpl(Strategy.HASH, number, list(expressions))

    @staticmethod
    def of(strategy: Strategy, number: int, *expressions: Expression) -> DistributionImpl:
        """Create a distribution by the given strategy.

        Args:
            strategy: The strategy to use
            number: The number of buckets
            expressions: The expressions to distribute by

        Returns:
            The created even distribution
        """
        return DistributionImpl(strategy, number, list(expressions))

    @staticmethod
    def fields(strategy: Strategy, number: int, *field_names: List[str]) -> DistributionImpl:
        """Create a distribution on columns. Like distribute by (a) or (a, b), for complex like
            distributing by (func(a), b) or (func(a), func(b)), please use DistributionImpl.Builder to create.
            NOTE: a, b, c are column names.

            SQL syntax: distribute by hash(a, b) buckets 5
            fields(Strategy.HASH, 5, new String[]{"a"}, new String[]{"b"});

            SQL syntax: distribute by hash(a, b, c) buckets 10
            fields(Strategy.HASH, 10, new String[]{"a"}, new String[]{"b"}, new String[]{"c"});

            SQL syntax: distribute by EVEN(a) buckets 128
            fields(Strategy.EVEN, 128, new String[]{"a"});

        Args:
            strategy: The strategy to use
            number: The number of buckets
            field_names: The field names to distribute by.

        Returns:
            The created even distribution
        """
        expressions = [NamedReference.field(name) for names in field_names for name in names]
        return Distributions.of(strategy, number, *expressions)


Distributions.NONE = DistributionImpl(Strategy.NONE, 0, Expression.EMPTY_EXPRESSION)
Distributions.HASH = DistributionImpl(Strategy.HASH, 0, Expression.EMPTY_EXPRESSION)
Distributions.RANGE = DistributionImpl(Strategy.RANGE, 0, Expression.EMPTY_EXPRESSION)
