from .binarysearchtree import BinarySearchTree
from ..store import Store
from ..error import StorageException
from ..error import ErrorType
from .inmemoryiterator import InMemoryIterator


class InMemoryStore(Store):
    def __init__(self, store_name):
        self.store_name = store_name
        self.database = BinarySearchTree()
        self.size = 0

    def set(self, key: str, value: str) -> None:
        """
        :param key: the key to be set
        :param value: the value to be inserted or update
        update size: 16 is the total size for key_size, val_size, timestamp
        updating size to track the size of sstable file
        if flush the memory table into ss table
        :return:
        """
        if self.database.contain(key):
            prev_value = self.database.get(key)
            self.database.update(key, value)
            self.size -= len(prev_value) + len(value)
        else:
            self.database.insert(key, value)
            self.size += 16 + len(key) + len(value)

    def contain_key(self, key: str) -> bool:
        return self.database.contain(key)

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
        self.size -= 1

    def clean(self) -> None:
        self.database = BinarySearchTree()
        self.size = 0

    def get_size(self) -> int:
        return self.size
