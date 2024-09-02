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

from enum import Enum


class Strategy(Enum):
    """
    An enum that defines the distribution strategy.
    The following strategies are supported:
    Hash: Uses the hash value of the expression to distribute data.
    Range: Uses the range of the expression specified to distribute data.
    Even: Distributes data evenly across partitions.
    """

    NONE = "NONE"
    """No distribution strategy. This is the default strategy. Will depend on the allocation strategy of the 
    underlying system."""

    HASH = "HASH"
    """Uses the hash value of the expression to distribute data."""

    RANGE = "RANGE"
    """Uses the range of the expression specified to distribute data. The range is specified using the rangeStart and 
    rangeEnd properties."""

    EVEN = "EVEN"
    """Distributes data evenly across partitions."""

    @staticmethod
    def get_by_name(name: str) -> 'Strategy':
        """
        Get the distribution strategy by name.

        Args:
            name: The name of the distribution strategy.

        Returns:
            The distribution strategy.
        """
        for strategy in Strategy:
            if strategy.name.lower() == name.lower():
                return strategy
        raise ValueError(f"Invalid distribution strategy: {name}. Valid values are: {[s.name for s in Strategy]}")
