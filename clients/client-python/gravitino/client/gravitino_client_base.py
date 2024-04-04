import json
import logging
from abc import ABC, abstractmethod
from urllib.parse import urlparse

from gravitino.client.auth_data_provider import AuthDataProvider
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.client.gravitino_version import GravitinoVersion
from gravitino.name_identifier import NameIdentifier
from gravitino.utils import HTTPClient

# from gravitino_client import RESTClient, HTTPClient, AuthDataProvider, SimpleTokenProvider, OAuth2TokenProvider, \
#     KerberosTokenProvider, NameIdentifier, GravitinoMetalake, NoSuchMetalakeException, DTOConverters, \
#     VersionResponse, MetalakeResponse, ErrorHandlers, GravitinoVersion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GravitinoClientBase():

    API_METALAKES_LIST_PATH = "api/metalakes"
    API_METALAKES_IDENTIFIER_PATH = f"{API_METALAKES_LIST_PATH}/"

    def __init__(self, uri: str):
        self.rest_client = HTTPClient(uri, timeout=10)
        #.builder({}).uri(uri).with_auth_data_provider(auth_data_provider).build()

    def load_metalake(self, ident: NameIdentifier) -> GravitinoMetalake:
        NameIdentifier.check_metalake(ident)

        resp = self.rest_client.get(self.API_METALAKES_IDENTIFIER_PATH + ident.name(),)
        object = json.load(resp.body)

        return object

    def get_version(self) -> GravitinoVersion:
        resp = self.rest_client.get("api/version")#, VersionResponse, {}, ErrorHandlers.rest_error_handler())
        resp.validate()
        return GravitinoVersion(resp.get_version())

    def close(self):
        if self.rest_client:
            try:
                self.rest_client.close()
            except Exception as e:
                logger.warning("Failed to close the HTTP REST client", exc_info=e)


# class GravitinoClientBuilder(ABC):
#
#     def __init__(self, uri: str):
#         self.uri = uri
#         self.auth_data_provider = None
#
#     def with_simple_auth(self) -> 'GravitinoClientBuilder':
#         self.auth_data_provider = SimpleTokenProvider()
#         return self
#
#     def with_oauth(self, data_provider: OAuth2TokenProvider) -> 'GravitinoClientBuilder':
#         self.auth_data_provider = data_provider
#         return self
#
#     def with_kerberos_auth(self, data_provider: KerberosTokenProvider) -> 'GravitinoClientBuilder':
#         try:
#             host = urlparse(self.uri).hostname
#             data_provider.set_host(host)
#         except Exception as e:
#             raise ValueError("URI has the wrong format") from e
#         self.auth_data_provider = data_provider
#         return self
#
#     @abstractmethod
#     def build(self):
#         pass
