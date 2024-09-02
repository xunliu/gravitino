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

from gravitino.api.rel.expressions.partitions.partition import Partition


class Partitions:
    """
    The helper class for partition expressions.
    """

    EMPTY_PARTITIONS: List[Partition] = []

    @staticmethod
    def range(name: str, upper: Literal, lower: Literal, properties: Dict[str, str]) -> RangePartition:
        return RangePartition(name, upper, lower, properties)

    @staticmethod
    def list(name: str, lists: List[List[Literal]], properties: Dict[str, str]) -> ListPartition:
        return ListPartition(name, lists, properties)

    @staticmethod
    def identity(name: Optional[str], field_names: List[List[str]], values: List[Literal],
                 properties: Optional[Dict[str, str]] = None) -> IdentityPartition:
        return IdentityPartition(name, field_names, values, properties or {})

    @staticmethod
    def identity_auto(field_names: List[List[str]], values: List[Literal]) -> IdentityPartition:
        return Partitions.identity(None, field_names, values, {})
