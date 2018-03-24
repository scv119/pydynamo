from typing import Any
from abc import ABC
from abc import abstractmethod


class StorageEngine(ABC):
    @abstractmethod
    def create_store(self, store_name: str) -> Any:
        pass

    @abstractmethod
    def get_store(self, store_name: str) -> Any:
        pass
