from .binarysearchtree import BinarySearchTree
from ..store import Store
from ..error import StorageException
from ..error import ErrorType
from .inmemoryiterator import InMemoryIterator


class InMemoryStore(Store):
    INT_SIZE = 4
    TIMESTAMP_SIZE = 8
    INDICATOR_SIZE = 4

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
        if self.database.contain_not_removed(key):
            prev_value = self.database.get(key)
            self.database.update(key, value)
            self.size = self.size - len(prev_value) + len(value)
        elif self.database.contain(key):
            self.database.update(key, value)
            self.size = self.size + self.INT_SIZE + len(value)
        else:
            self.database.insert(key, value)
            self.size += self.INDICATOR_SIZE + self.INT_SIZE \
                + len(key) + self.INT_SIZE + len(value) + self.TIMESTAMP_SIZE

    def contain_key(self, key: str) -> bool:
        return self.database.contain(key)

    def is_removed(self, key: str) -> bool:
        if self.database.contain_not_removed(key):
            return False
        return True

    def get(self, key: str) -> str:
        if not self.database.contain_not_removed(key):
            raise StorageException(ErrorType.NOT_FOUND,
                                   "This key cannot be found in store.")
        return self.database.get(key)

    def iterator(self) -> InMemoryIterator:
        return InMemoryIterator(self.database)

    def remove(self, key: str) -> None:
        if self.database.contain(key):
            if self.database.contain_not_removed(key):
                value = self.database.get(key)
                self.database.remove(key)
                self.size = self.size - self.INT_SIZE - len(value)
        else:
            self.database.remove(key)
            self.size = self.size + self.INDICATOR_SIZE + \
                self.INT_SIZE + len(key)

    def clean(self) -> None:
        self.database = BinarySearchTree()
        self.size = 0

    def get_size(self) -> int:
        return self.size
