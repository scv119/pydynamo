from ..memory.inmemorystore import InMemoryStore
from ..error import StorageException
from ..error import ErrorType
from time import time
from .sstable import SSTable
from struct import pack
from typing import Any
from ..store import Store
from typing import List
from .rwlock import RWLock
import os


class DiskStore(Store):
    INT_SIZE = 4
    TIMESTAMP_SIZE = 8

    def __init__(self, store_name: str, path: str,
                 mem_size_threshold: int) -> None:
        """
        :param store_name: name of store
        :param mem_size_threshold: the threshold to flush mem_table to disk
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
        self.rwlock = RWLock()
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        self.ss_table_dir = os.path.join(self.path, "sstable")
        self.index_dir = os.path.join(self.path, "index")
        if not os.path.exists(self.ss_table_dir):
            os.mkdir(self.ss_table_dir)
        if not os.path.exists(self.index_dir):
            os.mkdir(self.index_dir)
        self.ss_tables: dict = {}
        self.mem_size_threshold = mem_size_threshold

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

    def flush(self) -> None:
        """
        :return: None
        if it is not flushing and size of expected sstable
        file reaches threshold, set start_flush to True to
        block read and write flush mem_table into disk,
        add sstable in ss_tables update id, clean mem_table
        and index_table.
        after it, set start_flush back to False
        """
        ss_table_name = self.store_name + str(self.id)
        self.ss_tables[ss_table_name] = self.create_sstable(self.mem_table, {})
        self.id += 1
        self.mem_table.clean()
        self.index_table = []

    def create_sstable(self, mem_table: InMemoryStore,
                       time_stamp: dict) -> SSTable:
        """
        :param mem_table: table in memory
        :param time_stamp: time stamp corresponding to mem_table
        :return: create a sstable
        """
        size = self._flush_mem_to_disk(mem_table, time_stamp)
        new_ss_table = SSTable(self.store_name, self.id, size,
                               self.index_table, self.path, self.last_index)
        return new_ss_table

    def set(self, key: str, value: str) -> Any:
        """
        :param key: key to add
        :param value: value corresponding to key
        :return: add the set of data
        """
        self.rwlock.wlock_acquire()
        print("writing " + key + " " + value)
        self.mem_table.set(key, value)
        if self.mem_table.get_size() >= self.mem_size_threshold:
            self.flush()
            print("flush")
        self.rwlock.release()

    def get(self, key: str) -> Any:
        """
        :param key: the key to be found
        :return: the value corresponding to the key. if not found, return None
        check whether it starts flushing. if flushing, raise an exception.
        otherwise, read mem_table, return value if found.
        if not found in mem_table, read sstables and find
        the value with latest timestamp
        """
        self.rwlock.rlock_acquire()
        print("reading " + key)
        value = None
        if self.mem_table.contain_key(key):
            value = self.mem_table.get(key)
        else:
            timestamp = 0
            for ss_table in self.ss_tables:
                if self.ss_tables[ss_table].contain(key):
                    cur_timestamp = self.ss_tables[ss_table].get_timestamp(key)
                    if cur_timestamp > timestamp:
                        timestamp = cur_timestamp
                        value = self.ss_tables[ss_table].get(key)
        self.rwlock.release()
        return value

    def iterator(self) -> Any:
        pass

    def remove(self, key: str) -> Any:
        pass

    def _flush_mem_to_disk(self, mem_table: InMemoryStore,
                           time_stamp: dict) -> int:
        """
        flush memory table into disk
        :param mem_table: table in memory
        :param time_stamp: the time stamp corresponding to data in mem_table,
        if the mem_table is the newest mem_table, time stamp is empty.
        index_table: in memory
        data : tuple (key, offset in index_file for the key)
        sstable_file: in disk
        data: key_size(4 byte) + key + value_size(4 byte) +
        value + time_stamp(8 byte)
        index_file: in memory
        data: key_size(4 byte) + key + offset(4 byte)
        :return: size of sstable file
        """
        iterator = mem_table.iterator()
        ss_table_file_name = self.store_name + str(self.id) + ".ss"
        index_file_name = self.store_name + str(self.id) + ".index"
        ss_table_file = open(os.path.join(self.ss_table_dir,
                                          ss_table_file_name), "wb+")
        index_file = open(os.path.join(self.index_dir, index_file_name), "wb+")
        offset = 0
        key_index = 0
        key_size = 0
        val_size = 0
        interval = self._index_interval(self.mem_table.get_size())
        index_offset = 0
        while iterator.valid():
            iterator.next()
            key = iterator.key()
            value = iterator.value()
            if key_index % interval == 0:
                self.index_table.append((key, pack("i", index_offset)))
            key_size = len(key.encode())
            val_size = len(value.encode())
            bin_key_size = pack("i", key_size)
            bin_val_size = pack("i", val_size)
            bin_offset = pack("i", offset)
            index_file.write(bin_key_size)
            index_file.write(key.encode())
            index_file.write(bin_offset)
            index_offset += self.INT_SIZE + key_size + self.INT_SIZE
            ss_table_file.write(bin_key_size)
            ss_table_file.write(key.encode())
            ss_table_file.write(bin_val_size)
            ss_table_file.write(value.encode())
            if len(time_stamp) != 0:
                ss_table_file.write(time_stamp[key])
            else:
                timestamp = int(time() * 1000000)
                bin_timestamp = pack("q", timestamp)
                ss_table_file.write(bin_timestamp)
            offset += self.INT_SIZE + key_size + \
                self.INT_SIZE + val_size + self.TIMESTAMP_SIZE
            key_index += 1
        self.last_index = index_offset - self.INT_SIZE -\
            key_size - self.INT_SIZE
        index_file.close()
        ss_table_file.close()
        return offset

    def _flush_to_disk(self) -> int:
        """
        flush self.mem_table to disk
        :return: size of new created sstable file
        """
        return self._flush_mem_to_disk(self.mem_table, {})

    def _index_interval(self, size) -> int:
        """
        :param size: size of a in memory table
        :return: the index interval calculated based on index_ratio
        """
        if size == 0:
            return 0
        index_counter = size * self.index_ratio
        interval = size / index_counter
        return interval

    def _merge_sstable(self, sstable1: SSTable, sstable2: SSTable) -> None:
        """
        :param sstable1: one sstable file
        :param sstable2: another sstable file
        :return: create merged sstable file
        it is not completed yet.
        """
        time_stamp = {}
        temp_mem_store = InMemoryStore("temp")
        iterator1 = sstable1.disk_iterator
        iterator2 = sstable2.disk_iterator
        while iterator1.valid():
            iterator1.next()
            cur_key = iterator1.key()
            cur_val = iterator1.value()
            temp_mem_store.set(cur_key, cur_val)
            time_stamp[cur_key] = iterator1.timestamp()
        while iterator2.valid():
            iterator2.next()
            cur_key = iterator2.key()
            cur_val = iterator2.value()
            if temp_mem_store.contain_key(cur_key):
                iterator1.seek(cur_key)
                time1 = iterator1.timestamp()
                time2 = iterator2.timestamp()
                if time2 > time1:
                    temp_mem_store.set(cur_key, cur_val)
                    time_stamp[cur_key] = iterator2.timestamp()
        self._flush_mem_to_disk(temp_mem_store, time_stamp)
        # TODO
