import json
import logging
from typing import List, Dict

from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.dto.requests.metalake_create_request import MetalakeCreateRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.metalake_list_response import MetalakeListResponse
from gravitino.dto.responses.metalake_response import MetalakeResponse
from gravitino.dto.requests.metalake_updates_request import MetalakeUpdatesRequest
from gravitino.meta_change import MetalakeChange
from gravitino.name_identifier import NameIdentifier
from gravitino.supports_metalakes import SupportsMetalakes
from gravitino.dto.dto_converters import DTOConverters

logger = logging.getLogger(__name__)

class GravitinoAdminClient(GravitinoClientBase, SupportsMetalakes):

    def __init__(self, uri):
        super().__init__(uri)

    def list_metalakes(self) -> List[GravitinoMetalake]:
        resp = self.rest_client.get(
            self.API_METALAKES_LIST_PATH)  # , MetalakeListResponse, {}, ErrorHandlers.metalake_error_handler())
        # resp.validate()

        object = json.loads(resp.body, object_hook=MetalakeListResponse.object_hook)

        return object.metalakes  # [DTOConverters.to_meta_lake(o, rest_client) for o in resp.get_metalakes()]

    def create_metalake(self, ident: NameIdentifier, comment: str, properties: Dict[str, str]) -> GravitinoMetalake:
        NameIdentifier.check_metalake(ident)

        req = MetalakeCreateRequest(ident.name, comment, properties)
        req.validate()

        # headers = {'Content-Type': 'application/json', 'Accept': 'application/vnd.gravitino.v1+json'}
        resp = self.rest_client.post(self.API_METALAKES_LIST_PATH, req)#json.dumps(req, default=vars)) #req)
        # resp.validate()
        # json2 = resp.json().from_json()#.json()
        metalakeResponse = MetalakeResponse.from_json(resp.body)

        # object = json.loads(resp.body, object_hook=MetalakeCreateRequest.from_json_string)

        return DTOConverters.to_meta_lake(metalakeResponse.metalake, self.rest_client)

    def alter_metalake(self, ident: NameIdentifier, *changes: MetalakeChange) -> GravitinoMetalake:
        NameIdentifier.check_metalake(ident)

        reqs = [DTOConverters.to_metalake_update_request(change) for change in changes]
        updates_request = MetalakeUpdatesRequest(reqs)
        updates_request.validate()

        resp = self.rest_client.put(self.API_METALAKES_IDENTIFIER_PATH + ident.name, updates_request) #, MetalakeResponse, {}, ErrorHandlers.metalake_error_handler())
        # resp.validate()
        metalake_response = MetalakeResponse.from_json(resp.body)

        return DTOConverters.to_meta_lake(metalake_response.metalake, self.rest_client)

    def drop_metalake(self, ident: NameIdentifier) -> bool:
        NameIdentifier.check_metalake(ident)

        try:
            resp = self.rest_client.delete(
                self.API_METALAKES_IDENTIFIER_PATH + ident.name)  # , DropResponse, {}, ErrorHandlers.metalake_error_handler())
            dropResponse = DropResponse.from_json(resp.body)

            return dropResponse.dropped()

        except Exception as e:
            logger.warning(f"Failed to drop metadata ", e)
            return False

    @staticmethod
    def builder(uri: str) -> 'Builder':
        return GravitinoAdminClient.Builder(uri)

    class Builder(GravitinoClientBase.Builder):

        def __init__(self, uri: str):
            super().__init__(uri)

        def build(self) -> 'GravitinoAdminClient':
            if not self.uri or not self.uri.strip():
                raise ValueError("The argument 'uri' must be a valid URI")

            return GravitinoAdminClient(self.uri) #, self.auth_data_provider