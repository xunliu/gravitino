from abc import ABC
from typing import List
from dataclasses import dataclass
from typing import Optional
from abc import ABC, abstractmethod
import logging
from abc import ABC

from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.responses.base_response import BaseResponse

logger = logging.getLogger(__name__)

# @dataclass
class MetalakeListResponse(BaseResponse):
    metalakes: List[MetalakeDTO]

    def __init__(self, metalakes: List[MetalakeDTO]):
        super().__init__(code=0)
        self.metalakes = metalakes

    @classmethod
    def default(cls):
        return cls(metalakes=[])

    def validate(self):
        super().validate()

        if self.metalakes is None:
            raise ValueError("metalakes must be non-null")

        for metalake in self.metalakes:
            if not metalake.name:
                raise ValueError("metalake 'name' must not be null and empty")
            if not metalake.auditInfo:
                raise ValueError("metalake 'audit' must not be null")

    def object_hook(d):
        return MetalakeListResponse(d['metalakes'])