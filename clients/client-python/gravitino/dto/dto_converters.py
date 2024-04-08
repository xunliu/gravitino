"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from gravitino.api.catalog import Catalog
from gravitino.api.catalog_change import CatalogChange
from gravitino.api.fileset_change import FilesetChange
from gravitino.api.schema_change import SchemaChange
from gravitino.catalog.fileset_catalog import FilesetCatalog
from gravitino.dto.catalog_dto import CatalogDTO
from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest
from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.api.metalake_change import MetalakeChange
from gravitino.dto.requests.schema_update_request import SchemaUpdateRequest
from gravitino.utils import HTTPClient


class DTOConverters:
    """Utility class for converting between DTOs and domain objects."""

    @staticmethod
    def to_metalake_update_request(change: MetalakeChange) -> object:
        # Assuming MetalakeUpdateRequest has similar nested class structure for requests
        if isinstance(change, MetalakeChange.RenameMetalake):
            return MetalakeUpdateRequest.RenameMetalakeRequest(change.newName)
        elif isinstance(change, MetalakeChange.UpdateMetalakeComment):
            return MetalakeUpdateRequest.UpdateMetalakeCommentRequest(change.newComment)
        elif isinstance(change, MetalakeChange.SetProperty):
            return MetalakeUpdateRequest.SetMetalakePropertyRequest(change.property, change.value)
        elif isinstance(change, MetalakeChange.RemoveProperty):
            return MetalakeUpdateRequest.RemoveMetalakePropertyRequest(change.property)
        else:
            raise ValueError(f"Unknown change type: {type(change).__name__}")

    @staticmethod
    def to_catalog(catalog: CatalogDTO, client: HTTPClient):

        # if catalog_type == Catalog.Type.RELATIONAL:
        #     return RelationalCatalog.builder() \
        #         .withName(catalog.name()) \
        #         .withType(catalog.type()) \
        #         .withProvider(catalog.provider()) \
        #         .withComment(catalog.comment()) \
        #         .withProperties(catalog.properties()) \
        #         .withAudit(catalog.auditInfo()) \
        #         .withRestClient(client) \
        #         .build()

        if catalog.type == Catalog.Type.FILESET:
            return FilesetCatalog(name=catalog.name,
                                  type = catalog.type,
                                  provider =catalog.provider,
                                  comment=catalog.comment,
                                  properties=catalog.properties,
                                  audit=catalog.audit,
                                  rest_client = client)

        # elif catalog_type == Catalog.Type.MESSAGING:
        #     return MessagingCatalog.builder() \
        #         .withName(catalog.name()) \
        #         .withType(catalog.type()) \
        #         .withProvider(catalog.provider()) \
        #         .withComment(catalog.comment()) \
        #         .withProperties(catalog.properties()) \
        #         .withAudit(catalog.auditInfo()) \
        #         .withRestClient(client) \
        #         .build()

        else:
            raise NotImplementedError("Unsupported catalog type: " + str(catalog.type()))

    @staticmethod
    def to_catalog_update_request(change: CatalogChange):
        if isinstance(change, CatalogChange.RenameCatalog):
            return CatalogUpdateRequest.RenameCatalogRequest(change.new_name)
        elif isinstance(change, CatalogChange.UpdateCatalogComment):
            return CatalogUpdateRequest.UpdateCatalogCommentRequest(change.new_comment)
        elif isinstance(change, CatalogChange.SetProperty):
            return CatalogUpdateRequest.SetCatalogPropertyRequest(change.property, change.value)
        elif isinstance(change, CatalogChange.RemoveProperty):
            return CatalogUpdateRequest.RemoveCatalogPropertyRequest(change.property)
        else:
            raise ValueError(f"Unknown change type: {type(change).__name__}")

    # @staticmethod
    # def to_schema_update_request(change: SchemaChange):
    #     if isinstance(change, SchemaChange.SetProperty):
    #         return SchemaUpdateRequest.SetSchemaPropertyRequest(change.property, change.value)
    #     elif isinstance(change, SchemaChange.RemoveProperty):
    #         return SchemaUpdateRequest.RemoveSchemaPropertyRequest(change.property)
    #     else:
    #         raise ValueError(f"Unknown change type: {type(change).__name__}")

    # @staticmethod
    # def to_fileset_update_request(change: FilesetChange):
    #     if isinstance(change, FilesetChange.RenameFileset):
    #         return CatalogUpdateRequest.RenameCatalogRequest(change.new_name)
    #     elif isinstance(change, FilesetChange.UpdateFilesetComment):
    #         return CatalogUpdateRequest.UpdateCatalogCommentRequest(change.new_comment)
    #     elif isinstance(change, FilesetChange.SetProperty):
    #         return CatalogUpdateRequest.SetCatalogPropertyRequest(change.property, change.value)
    #     elif isinstance(change, FilesetChange.RemoveProperty):
    #         return CatalogUpdateRequest.RemoveCatalogPropertyRequest(change.property)
    #     else:
    #         raise ValueError(f"Unknown change type: {type(change).__name__}")
