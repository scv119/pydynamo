from typing import Any
from abc import ABC
from abc import abstractmethod


class Store(ABC):
    @abstractmethod
    def set(self, key: str, value: str) -> Any:
        pass

    @abstractmethod
    def get(self, key: str) -> Any:
        pass

    @abstractmethod
    def iterator(self) -> Any:
        pass

    @abstractmethod
    def remove(self, key:str) -> Any:
        pass