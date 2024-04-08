import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict

from gravitino.api.catalog import Catalog
from gravitino.api.schema_change import SchemaChange
from gravitino.api.supports_schemas import SupportsSchemas
# from gravitino.catalog.fileset_catalog import FilesetCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.catalog_dto import CatalogDTO
# from gravitino.dto.dto_converters import DTOConverters
from gravitino.dto.requests.schema_create_request import SchemaCreateRequest
from gravitino.dto.requests.schema_update_request import SchemaUpdateRequest
from gravitino.dto.requests.schema_updates_request import SchemaUpdatesRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.schema_response import SchemaResponse
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient

logger = logging.getLogger(__name__)


# @dataclass
class BaseSchemaCatalog(CatalogDTO, SupportsSchemas):  # SupportsSchemas
    name: str
    type: Catalog.Type
    provider: str
    comment: str
    properties: Dict[str, str]
    audit: AuditDTO
    rest_client: HTTPClient

    def __init__(self, name: str = None, type: Catalog.Type = Catalog.Type.UNSUPPORTED, provider: str = None,
                 comment: str = None, properties: Dict[str, str] = None, audit: AuditDTO = None,
                 rest_client: HTTPClient = None) -> None:
        super().__init__(name=name, type=type, provider=provider, comment=comment, properties=properties, audit=audit)
        self.rest_client = rest_client

    def as_schemas(self):
        return self

    def list_schemas(self, namespace: Namespace) -> [NameIdentifier]:
        Namespace.check_schema(namespace)
        resp = self.rest_client.get(BaseSchemaCatalog.format_schema_request_path(
            namespace))  # , EntityListResponse.class, Collections.emptyMap(), ErrorHandlers.schemaErrorHandler())
        entity_list_response = EntityListResponse.from_dict(resp.json())
        entity_list_response.validate()
        return entity_list_response.idents

    def create_schema(self, ident: NameIdentifier = None, comment: str = None, properties: Dict[str, str] = None):
        NameIdentifier.check_schema(ident)
        req = SchemaCreateRequest(ident.name, comment, properties)
        req.validate()

        resp = self.rest_client.post(BaseSchemaCatalog.format_schema_request_path(
            ident.namespace), json=req)  # , req, SchemaResponse.class, Collections.emptyMap(), ErrorHandlers.schemaErrorHandler())
        schema_response = SchemaResponse.from_json(resp.body, infer_missing=True)
        schema_response.validate()

        return schema_response.schema

    def load_schema(self, ident):
        NameIdentifier.check_schema(ident)
        resp = self.rest_client.get(BaseSchemaCatalog.format_schema_request_path(
            ident.namespace) + "/" + ident.name)  # , SchemaResponse.class, Collections.emptyMap(), ErrorHandlers.schemaErrorHandler())
        schema_response = SchemaResponse.from_json(resp.body, infer_missing=True)
        schema_response.validate()

        return schema_response.schema

    def alter_schema(self, ident, *changes):
        NameIdentifier.check_schema(ident)
        reqs = [BaseSchemaCatalog.to_schema_update_request(change) for change in changes]
        updatesRequest = SchemaUpdatesRequest(reqs)
        updatesRequest.validate()
        resp = self.rest_client.put(BaseSchemaCatalog.format_schema_request_path(
            ident.namespace) + "/" + ident.name)  # , updatesRequest, SchemaResponse.class, Collections.emptyMap(), ErrorHandlers.schemaErrorHandler())
        schema_response = SchemaResponse.from_json(resp.body, infer_missing=True)
        schema_response.validate()
        return schema_response.schema

    def drop_schema(self, ident, cascade: bool):
        NameIdentifier.check_schema(ident)
        try:
            params = {"cascade": str(cascade)}
            resp = self.rest_client.delete(
                BaseSchemaCatalog.format_schema_request_path(ident.namespace) + "/" + ident.name,
                params=params)  # Collections.singletonMap("cascade", str(cascade)), DropResponse.class, Collections.emptyMap(), ErrorHandlers.schemaErrorHandler())
            drop_resp = DropResponse.from_json(resp.body, infer_missing=True)
            drop_resp.validate()
            return drop_resp.dropped
        # except NonEmptySchemaException as e:
        #     raise e
        except Exception as e:
            logger.warning("Failed to drop schema {}", ident, e)
            return False

    @staticmethod
    def format_schema_request_path(ns: Namespace):
        return "api/metalakes/" + ns.level(0) + "/catalogs/" + ns.level(1) + "/schemas"

    @staticmethod
    def to_schema_update_request(change: SchemaChange):
        if isinstance(change, SchemaChange.SetProperty):
            return SchemaUpdateRequest.SetSchemaPropertyRequest(change.property, change.value)
        elif isinstance(change, SchemaChange.RemoveProperty):
            return SchemaUpdateRequest.RemoveSchemaPropertyRequest(change.property)
        else:
            raise ValueError(f"Unknown change type: {type(change).__name__}")
