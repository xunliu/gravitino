from dataclasses import dataclass

@dataclass
class VersionDTO:
    version: str = ""
    compileDate: str = ""
    gitCommit: str = ""