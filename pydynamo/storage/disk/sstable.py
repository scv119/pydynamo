from ..store import Store
from ..error import StorageException
from ..error import ErrorType
from .sstableiterator import SStableIterator
from typing import List


class SSTable(Store):
    INT_SIZE = 4
    TIMESTAMP_SIZE = 26

    def __init__(self, store_name: str, store_id: int,
                 size: int, index_table: List, path: str, last_index) -> None:
        """
        :param store_name: name of the store this sstable belongs to
        :param store_id: id of the sstable
        :param size: size of sstable file
        :param index_table: index table stored in memory
        :param path: the path the sstable and index files stored in
        :param last_index: the offset of last
        key-data-time set data in sstable file
        """
        self.store_name = store_name
        self.id = store_id
        self.size = size
        self.index_table = index_table
        self.path = path
        self.last_index = last_index
        self.diskIterator = self.iterator()

    def get(self, key: str) -> str:
        """
        :param key: the key user intends to find in sstable
        :return: the value corresponding to key
        """
        self.diskIterator.seek(key)
        return self.diskIterator.value()

    def get_timestamp(self, key: str) -> str:
        """
        :param key: the key user intends to find in sstable
        :return: the created time of this key-value set data
        """
        self.diskIterator.seek(key)
        return self.diskIterator.timestamp()

    def set(self, key: str, value: str):
        raise StorageException(ErrorType.NOT_FOUND,
                               "SSTable cannot be modified.")

    def iterator(self):
        """
        :return: return a iterator of this sstable
        """
        return SStableIterator(self.store_name,
                               self.id, self.size, self.index_table,
                               self.path, self.last_index)

    def remove(self, key: str) -> None:
        raise StorageException(ErrorType.NOT_FOUND,
                               "SSTable cannot be modified.")
