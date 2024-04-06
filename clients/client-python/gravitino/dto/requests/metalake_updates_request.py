from dataclasses import dataclass
from typing import List

from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.rest.rest_request import RESTRequest


@dataclass
class MetalakeUpdatesRequest(RESTRequest):
    updates: List[MetalakeUpdateRequest]

    def __init__(self, updates: List[MetalakeUpdateRequest]):
        self.updates = updates

    def validate(self):
        for update in self.updates:
            update.validate()
