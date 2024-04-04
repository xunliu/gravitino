from dataclasses import dataclass

from gravitino.dto import VersionDTO


@dataclass
class GravitinoVersion(VersionDTO):
    def __init__(self, versionDTO):
        super().__init__(versionDTO.version, versionDTO.compile_date, versionDTO.git_commit)
