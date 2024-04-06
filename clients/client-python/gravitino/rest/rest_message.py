from abc import ABC, abstractmethod
from dataclasses_json import DataClassJsonMixin


class RESTMessage(DataClassJsonMixin, ABC):

    @abstractmethod
    def validate(self):
        """
        Ensures that a constructed instance of a REST message is valid according to the REST spec.

        This is needed when parsing data that comes from external sources and the object might have
        been constructed without all the required fields present.
        """
        pass
