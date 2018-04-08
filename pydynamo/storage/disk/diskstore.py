from ..memory.inmemorystore import InMemoryStore
from ..error import StorageException
from ..error import ErrorType
from datetime import datetime
from .sstable import SSTable
from struct import pack
from typing import Any
from ..store import Store
from typing import List
import os


class DiskStore(Store):
    INT_SIZE = 4

    def __init__(self, store_name: str, path: str) -> None:
        """
        :param store_name: name of store
        :param path: the path where user intend to store file
        """
        self.store_name = store_name
        self.mem_table = InMemoryStore(store_name)
        self.id = 0
        self.index_ratio = 0.1
        self.index_table: List = []
        self.last_index = 0
        self.path = path
        self.path = os.path.join(self.path, store_name)
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        self.sstable_dir = os.path.join(self.path, "sstable")
        self.index_dir = os.path.join(self.path, "index")
        if not os.path.exists(self.sstable_dir):
            os.mkdir(self.sstable_dir)
        if not os.path.exists(self.index_dir):
            os.mkdir(self.index_dir)

    def set_index_ratio(self, new_index_ratio: float) -> None:
        """
        the size of index table in memory should be limited, so
        we set the index_ration to control the size of index table
        in memory.
        Default new_index_ration = 0.1
        it means 1 in 10 data will be stored in index table
        :param new_index_ratio: one in 1/new_index_ratio data
        will be stored in memory table
        """
        if new_index_ratio > 1 or new_index_ratio <= 0:
            raise StorageException(ErrorType.INVALID_INPUT,
                                   "This input is not valid. "
                                   "It should be between (0, 1]")
        self.index_ratio = new_index_ratio

    def create_sstable(self) -> SSTable:
        """
        :return: create a sstable
        """
        size = self._flush_to_disk()
        new_sstable = SSTable(self.store_name, self.id, size,
                              self.index_table, self.path, self.last_index)
        self.id += 1
        self.mem_table.clean()
        self.index_table = []
        return new_sstable

    def set(self, key: str, value: str) -> Any:
        pass

    def get(self, key: str) -> Any:
        pass

    def iterator(self) -> Any:
        pass

    def remove(self, key: str) -> Any:
        pass

    def _flush_to_disk(self) -> int:
        """
        flush memory table into disk
        index_table: in memory
        data : tuple (key, offset for the key)
        sstable_file: in disk
        data: key_size(4 byte) + key + value_size(4 byte) +
        value + time_size(4 byte) + current time
        index_file: in memory
        data: key_size(4 byte) + key + offset(4 byte)
        :return: return the size of sstable file
        """
        iterator = self.mem_table.iterator()
        sstable_file_name = self.store_name + str(self.id) + ".db"
        index_file_name = self.store_name + str(self.id) + ".index"
        sstable_file = open(os.path.join(self.sstable_dir,
                                         sstable_file_name), "wb")
        index_file = open(os.path.join(self.index_dir, index_file_name), "wb")
        offset = 0
        key_index = 0
        key_size = 0
        interval = self._index_interval(self.mem_table.get_size())
        while iterator.valid():
            iterator.next()
            key = iterator.key()
            value = iterator.value()
            if key_index % interval == 0:
                self.index_table.append((key, pack("i", offset)))
            key_size = len(key.encode())
            val_size = len(value)
            bin_key_size = pack("i", key_size)
            bin_val_size = pack("i", val_size)
            bin_offset = pack("i", offset)
            self.last_index += self.INT_SIZE + key_size + self.INT_SIZE
            index_file.write(bin_key_size)
            index_file.write(key.encode())
            index_file.write(bin_offset)
            sstable_file.write(bin_key_size)
            sstable_file.write(key.encode())
            sstable_file.write(bin_val_size)
            sstable_file.write(value.encode())
            timestamp = str(datetime.utcnow()).encode()
            timestamp_size = len(timestamp)
            bin_timestamp_size = pack("i", timestamp_size)
            sstable_file.write(bin_timestamp_size)
            sstable_file.write(timestamp)
            offset += self.INT_SIZE + key_size + \
                self.INT_SIZE + val_size + self.INT_SIZE + timestamp_size
            key_index += 1
        self.last_index = self.last_index - self.INT_SIZE \
            - key_size - self.INT_SIZE
        index_file.close()
        sstable_file.close()
        return offset

    def _index_interval(self, size) -> int:
        index_counter = size * self.index_ratio
        interval = size / index_counter
        return interval
