import json
import logging
from abc import ABC, abstractmethod
from typing import List, Dict

from gravitino.client.gravitino_client_base import GravitinoClientBase
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.dto.responses.metalake_list_response import MetalakeListResponse
from gravitino.meta_change import MetalakeChange
from gravitino.name_identifier import NameIdentifier
from gravitino.supports_metalakes import SupportsMetalakes


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
        pass
    #     NameIdentifier.check_metalake(ident)
    #
    #     req = MetalakeCreateRequest(ident.name(), comment, properties)
    #     req.validate()
    #
    #     resp = rest_client.post(API_METALAKES_LIST_PATH, req, MetalakeResponse, {}, ErrorHandlers.metalake_error_handler())
    #     resp.validate()
    #
    #     return DTOConverters.to_meta_lake(resp.get_metalake(), rest_client)
    #
    def alter_metalake(self, ident: NameIdentifier, *changes: MetalakeChange) -> GravitinoMetalake:
        pass
    #     NameIdentifier.check_metalake(ident)
    #
    #     reqs = [DTOConverters.to_metalake_update_request(change) for change in changes]
    #     updates_request = MetalakeUpdatesRequest(reqs)
    #     updates_request.validate()
    #
    #     resp = rest_client.put(API_METALAKES_IDENTIFIER_PATH + ident.name(), updates_request, MetalakeResponse, {}, ErrorHandlers.metalake_error_handler())
    #     resp.validate()
    #
    #     return DTOConverters.to_meta_lake(resp.get_metalake(), rest_client)

    def drop_metalake(self, ident: NameIdentifier) -> bool:
        NameIdentifier.check_metalake(ident)

        try:
            resp = self.rest_client.delete(
                self.API_METALAKES_IDENTIFIER_PATH + ident.name())  # , DropResponse, {}, ErrorHandlers.metalake_error_handler())
            resp.validate()
            return resp.dropped()

        except Exception as e:
            # logger.warning(f"Failed to drop metadata {ident}", e)
            return False
