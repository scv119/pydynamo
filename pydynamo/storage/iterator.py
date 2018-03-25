from abc import ABC
from abc import abstractmethod


class Iterator(ABC):
    @abstractmethod
    def seek(self, key: str) -> None:
        """
        seek to a position in the iterator
        :param key: the key in the position
        :return: None
        """
        pass

    @abstractmethod
    def seek_to_first(self) -> None:
        """
        seek to the beginning of iterator
        :return: the beginning of iterator
        """
        pass

    @abstractmethod
    def valid(self) -> bool:
        """
        check whether there is a valid next element
        :return: True if there is a valid next element, otherwise False
        """
        pass

    @abstractmethod
    def next(self) -> None:
        """
        iterator points to next element
        :return: None
        """
        pass

    @abstractmethod
    def value(self) -> str:
        """
        return the value of element iterator points to
        :return: the value of element iterator points to
        """
        pass

    @abstractmethod
    def key(self) -> str:
        """
        return the key of element iterator points to
        :return: the key of element iterator points to
        """
        pass
