from typing import Any
from abc import ABC
from abc import abstractmethod


class StorageEngine(ABC):
    @abstractmethod
    def create_store(self, store_name: str) -> Any:
        """
        create a store
        :param store_name:
        :return: created store named store_name
        """
        pass

    @abstractmethod
    def get_store(self, store_name: str) -> Any:
        """
        get a store named store_name
        :param store_name:
        :return: store named store_name
        """
        pass
