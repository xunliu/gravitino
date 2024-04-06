from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.dto.metalake_dto import MetalakeDTO
from gravitino.dto.requests.metalake_update_request import MetalakeUpdateRequest
from gravitino.meta_change import MetalakeChange


class DTOConverters:

    @staticmethod
    def to_meta_lake(metalake: MetalakeDTO, client) -> GravitinoMetalake:
        return GravitinoMetalake.builder() \
            .with_name(metalake.name) \
            .with_comment(metalake.comment) \
            .with_properties(metalake.properties) \
            .with_audit(metalake.audit) \
            .with_rest_client(client) \
            .build()

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
