from dataclasses import dataclass
from typing import List

from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.rest.rest_request import RESTRequest


@dataclass
class MetalakeUpdatesRequest(RESTRequest):
    """
    Represents a request containing multiple Metalake updates.
    """
    updates: List[MetalakeUpdateRequest]

    def __init__(self, updates: List[MetalakeUpdateRequest]):
        """
        Constructor for MetalakeUpdatesRequest.
        @param updates The list of Metalake update requests.
        """
        self.updates = updates

    def validate(self):
        """
        Validates each request in the list.
        @throws IllegalArgumentException if validation of any request fails.
        """
        for update in self.updates:
            update.validate()
