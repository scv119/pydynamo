from ..store import Store
from ..error import StorageException
from ..error import ErrorType
from .sstableiterator import SStableIterator
from .diskiterator import DiskIterator
from typing import List
import os


class SSTable(Store):
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
        self.disk_iterator = self.iterator()

    def contain(self, key: str) -> bool:
        return self.disk_iterator.contain(key)

    def get(self, key: str) -> str:
        """
        :param key: the key user intends to find in sstable
        :return: the value corresponding to key
        """
        self.disk_iterator.seek(key)
        return self.disk_iterator.value()

    def get_timestamp(self, key: str) -> int:
        """
        :param key: the key user intends to find in sstable
        :return: the created time of this key-value set data
        """
        self.disk_iterator.seek(key)
        return self.disk_iterator.timestamp()

    def set(self, key: str, value: str):
        raise StorageException(ErrorType.ACTION_FORBIDDEN,
                               "SSTable cannot be modified.")

    def iterator(self):
        """
        :return: return a iterator of this sstable
        """
        return SStableIterator(self.store_name,
                               self.id, self.size, self.index_table,
                               self.path, self.last_index)

    def diskiterator(self):
        return DiskIterator(self.store_name, self.id,
                            self.size, self.index_table,
                            self.path, self.last_index)

    def remove(self, key: str) -> None:
        raise StorageException(ErrorType.ACTION_FORBIDDEN,
                               "SSTable cannot be modified.")

    def clean(self) -> None:
        ss_table_dir = os.path.join(self.path, "sstable")
        index_dir = os.path.join(self.path, "index")
        ss_table_file_name = self.store_name + str(self.id) + ".ss"
        index_file_name = self.store_name + str(self.id) + ".index"
        sstable_path = os.path.join(ss_table_dir, ss_table_file_name)
        index_path = os.path.join(index_dir, index_file_name)
        if os.path.exists(sstable_path):
            os.remove(sstable_path)
        if os.path.exists(index_path):
            os.remove(index_path)
