from .binarysearchtree import BinarySearchTree
from ..store import Store
from ..error import StorageException
from ..error import ErrorType
from .inmemoryiterator import InMemoryIterator


class InMemoryStore(Store):
    def __init__(self, store_name):
        self.store_name = store_name
        self.database = BinarySearchTree()

    def set(self, key: str, value: str) -> None:
        if self.database.contain(key):
            self.database.update(key, value)
        else:
            self.database.insert(key, value)

    def get(self, key: str) -> str:
        if not self.database.contain(key):
            raise StorageException(ErrorType.NOT_FOUND,
                                   "This key cannot be found in store.")
        return self.database.get(key)

    def iterator(self) -> InMemoryIterator:
        return InMemoryIterator(self.database)

    def remove(self, key: str) -> None:
        if not self.database.contain(key):
            raise StorageException(ErrorType.NOT_FOUND,
                                   "This key cannot be found in store.")
        self.database.remove_node(key)
