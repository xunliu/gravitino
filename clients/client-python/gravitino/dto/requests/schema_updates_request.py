from dataclasses import dataclass, field
from typing import Optional, List

from dataclasses_json import DataClassJsonMixin

from gravitino.dto.requests.schema_update_request import SchemaUpdateRequest


@dataclass
class SchemaUpdatesRequest(DataClassJsonMixin):
    updates: Optional[List[SchemaUpdateRequest]] = field(default_factory=list)

    def validate(self):
        if not self.updates:
            raise ValueError("Updates cannot be empty")
        for update_request in self.updates:
            update_request.validate()