from dataclasses import dataclass, field
from typing import Optional, List

from gravitino.dto.requests.fileset_update_request import FilesetUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FilesetUpdatesRequest(RESTRequest):
    """Request to represent updates to a fileset."""
    updates: Optional[List[FilesetUpdateRequest]] = field(default_factory=list)

    def validate(self):
        if not self.updates:
            raise ValueError("Updates cannot be empty")
        for update_request in self.updates:
            update_request.validate()