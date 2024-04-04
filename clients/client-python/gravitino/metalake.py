from abc import ABC, abstractmethod

from gravitino.auditable import Auditable


class Metalake(Auditable):
    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def comment(self):
        pass

    @property
    @abstractmethod
    def properties(self):
        pass
