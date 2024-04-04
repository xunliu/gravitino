from abc import abstractmethod

from gravitino import MetaLake


class SupportsMetalakes():

    @abstractmethod
    def listMetalakes(self) -> [MetaLake]:
        pass

    @abstractmethod
    def loadMetalake(self):
        pass