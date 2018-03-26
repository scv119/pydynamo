from typing import Any
from abc import ABC
from abc import abstractmethod


class Store(ABC):
    @abstractmethod
    def set(self, key: str, value: str) -> Any:
        """
        insert or update a tuple of key and value in store
        if this key exists, insert the key and value in store
        otherwise, find the element according to key and update its value
        :param key: key string for the element
        :param value: value string for the element
        :return: None
        """
        pass

    @abstractmethod
    def get(self, key: str) -> Any:
        """
        get the element according to given key
        :param key:
        :return: the element that has the key
        """
        pass

    @abstractmethod
    def iterator(self) -> Any:
        """
        get an iterator to traverse all elements in store
        :return: an iterator that can traverse all elements
        """
        pass

    @abstractmethod
    def remove(self, key: str) -> Any:
        """
        remove an element of a given key
        :param key: the removed element's key
        :return: None
        """
        pass
