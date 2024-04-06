from abc import ABC, abstractmethod
import logging
from typing import Dict, Any

# from gravitino.name_identifier import NameIdentifier
# from gravitino.utils import HTTPClient
# from gravitino_metalake import GravitinoMetalake
# from gravitino_version import GravitinoVersion
# from name_identifier import NameIdentifier
# from auth_data_provider import AuthDataProvider
# from http_client import HTTPClient
# from simple_token_provider import SimpleTokenProvider
# from oauth2_token_provider import OAuth2TokenProvider
# from kerberos_token_provider import KerberosTokenProvider
# from version_response import VersionResponse
# from metalake_response import MetalakeResponse
# from dto_converters import DTOConverters
# from error_handlers import ErrorHandlers
import collections

from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.gravitino_version import GravitinoVersion
from gravitino.name_identifier import NameIdentifier
from gravitino.utils import HTTPClient

logger = logging.getLogger(__name__)

class GravitinoClientBase:

    API_METALAKES_LIST_PATH = "api/metalakes"
    API_METALAKES_IDENTIFIER_PATH = f"{API_METALAKES_LIST_PATH}/"

    def __init__(self, uri: str):
        self.rest_client = HTTPClient(uri)

    def load_metalake(self, ident: NameIdentifier) -> GravitinoMetalake:
        pass
        # NameIdentifier.check_metalake(ident)
        #
        # resp = self.rest_client.get(GravitinoClientBase.API_METALAKES_IDENTIFIER_PATH + ident.name())
        # resp.validate()
        #
        # return DTOConverters.to_meta_lake(resp.get_metalake(), self.rest_client)

    def get_version(self) -> GravitinoVersion:
        resp = self.rest_client.get("api/version")
        resp.validate()

        return GravitinoVersion(resp.get_version())

    def close(self):
        if self.rest_client is not None:
            try:
                self.rest_client.close()
            except Exception as e:
                logger.warn("Failed to close the HTTP REST client", e)

    class Builder(ABC):

        def __init__(self, uri: str):
            self.uri = uri
            self.auth_data_provider = None

        # def with_simple_auth(self) -> 'Builder':
        #     self.auth_data_provider = SimpleTokenProvider()
        #     return self
        #
        # def with_oauth(self, data_provider: OAuth2TokenProvider) -> 'Builder':
        #     self.auth_data_provider = data_provider
        #     return self
        #
        # def with_kerberos_auth(self, data_provider: KerberosTokenProvider) -> 'Builder':
        #     try:
        #         if self.uri is not None:
        #             data_provider.set_host(URI(self.uri).get_host())
        #     except URISyntaxException as ue:
        #         raise ValueError("URI has the wrong format") from ue
        #     self.auth_data_provider = data_provider
        #     return self

        @abstractmethod
        def build(self):
            pass