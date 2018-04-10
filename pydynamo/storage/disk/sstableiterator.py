from ..error import StorageException
from ..error import ErrorType
from ..iterator import Iterator
from struct import unpack
import os
from typing import Tuple


class SStableIterator(Iterator):
    INT_SIZE = 4
    TIMESTAMP_SIZE = 8

    def __init__(self, store_name: str,
                 id: int, size: int, index_table, path, last_index) -> None:
        """
        :param store_name: the name of store the sstable belongs to
        :param id: the id of the sstable
        :param size: the size of sstable file
        :param index_table: the index table stored in memory for this sstable
        :param path: the path the sstable file and index file store in
        :param last_index: the offset of last
        key-data-time set data in sstable file
        """
        sstable_file_name = store_name + str(id) + ".db"
        index_file_name = store_name + str(id) + ".index"
        self.cur = None
        sstable_dir = os.path.join(path, "sstable")
        index_dir = os.path.join(path, "index")
        self.sstable_file = open(os.path.join(sstable_dir, sstable_file_name),
                                 "rb")
        self.index_file = open(os.path.join(index_dir, index_file_name), "rb")
        self.index_table = index_table
        self.cur_offset = 0
        self.file_size = size
        self.last_index = last_index
        self.dummy_first = True

    def __del__(self) -> None:
        """
        close sstable file and index file
        :return: None
        """
        self.sstable_file.close()
        self.index_file.close()

    def seek(self, key: str) -> None:
        """
        :param key: the key the iterator will point to
        :return:
        """
        if self.file_size == 0:
            raise StorageException(ErrorType.NOT_FOUND,
                                   "The key is not found in table.")
        index_range = self._get_range(key)
        if index_range[0] == index_range[1]:
            offset = index_range[0]
            self.index_file.seek(offset)
            key_size = unpack("i", self.index_file.read(self.INT_SIZE))[0]
            self.index_file.read(key_size)
            self.cur_offset = unpack("i",
                                     self.index_file.read(self.INT_SIZE))[0]
        else:
            offset = self._get_offset(key, index_range[0], index_range[1])
            if offset is None:
                raise StorageException(ErrorType.NOT_FOUND,
                                       "The key is not found in table.")
            self.cur_offset = offset

    def seek_to_first(self) -> None:
        """
        set the iterator point to the beginning
        :return: None
        """
        self.cur_offset = 0
        self.dummy_first = True

    def valid(self) -> bool:
        """
        :return: true if the iterator has next.
        Otherwise false. considering empty sstable
        """
        if self.cur_offset < 0:
            return False
        elif self.cur_offset == 0 and self.file_size == 0:
            return False
        else:
            if self.cur_offset >= self.file_size:
                return False
            self.sstable_file.seek(self.cur_offset)
            key_size_str = self.sstable_file.read(self.INT_SIZE)
            key_size = unpack("i", key_size_str)[0]
            self.sstable_file.read(key_size)
            val_size_str = self.sstable_file.read(self.INT_SIZE)
            val_size = unpack("i", val_size_str)[0]
            self.sstable_file.read(val_size)
            offset = self.cur_offset + self.INT_SIZE \
                + key_size + self.INT_SIZE + val_size + \
                self.TIMESTAMP_SIZE
            if offset >= self.file_size:
                return False
            return True

    def next(self) -> None:
        """
        if iterator has next, set the iterator points to next;
        otherwise, raise an exception
        :return: None
        """
        if self.file_size == 0:
            raise StorageException(ErrorType.NONE_POINTER,
                                   "There is no next")
        elif self.cur_offset == 0 and self.dummy_first:
            self.cur_offset = 0
            self.dummy_first = False
        elif self.cur_offset >= 0:
            self.sstable_file.seek(self.cur_offset)
            key_size_str = self.sstable_file.read(self.INT_SIZE)
            key_size = unpack("i", key_size_str)[0]
            self.sstable_file.read(key_size)
            val_size_str = self.sstable_file.read(self.INT_SIZE)
            val_size = unpack("i", val_size_str)[0]
            self.sstable_file.read(val_size)
            self.cur_offset = self.cur_offset + self.INT_SIZE + key_size \
                + self.INT_SIZE + val_size + self.TIMESTAMP_SIZE
            if self.cur_offset >= self.file_size:
                raise StorageException(ErrorType.NONE_POINTER,
                                       "There is no next")

    def value(self) -> str:
        """
        :return: the value of the key-value-time
        set data the iterator points to
        """
        self.sstable_file.seek(self.cur_offset)
        key_size_str = self.sstable_file.read(self.INT_SIZE)
        key_size = unpack("i", key_size_str)[0]
        self.sstable_file.read(key_size)
        val_size_str = self.sstable_file.read(self.INT_SIZE)
        val_size = unpack("i", val_size_str)[0]
        value = self.sstable_file.read(val_size).decode()
        return value

    def key(self) -> str:
        """
        :return: the key of the key-value-time set data the iterator points to
        """
        self.sstable_file.seek(self.cur_offset)
        key_size_str = self.sstable_file.read(self.INT_SIZE)
        key_size = unpack("i", key_size_str)[0]
        key = self.sstable_file.read(key_size).decode()
        return key

    def timestamp(self) -> str:
        """
        :return: the time of the key-value-time
        set data the iterator points to
        """
        self.sstable_file.seek(self.cur_offset)
        key_size_str = self.sstable_file.read(self.INT_SIZE)
        key_size = unpack("i", key_size_str)[0]
        self.sstable_file.read(key_size)
        val_size_str = self.sstable_file.read(self.INT_SIZE)
        val_size = unpack("i", val_size_str)[0]
        self.sstable_file.read(val_size)
        bin_time_stamp = self.sstable_file.read(self.TIMESTAMP_SIZE)
        time_stamp = unpack("q", bin_time_stamp)[0]
        return time_stamp

    def length(self) -> int:
        """
        :return: the length of the key-value-time
        set data the iterator points to
        """
        self.sstable_file.seek(self.cur_offset)
        key_size_str = self.sstable_file.read(self.INT_SIZE)
        key_size = unpack("i", key_size_str)[0]
        self.sstable_file.read(key_size)
        val_size_str = self.sstable_file.read(self.INT_SIZE)
        val_size = unpack("i", val_size_str)[0]
        length = self.INT_SIZE + key_size \
            + self.INT_SIZE + val_size \
            + self.TIMESTAMP_SIZE
        return length

    def get_cur_offset(self) -> int:
        """
        :return: the offset of data the iterator currently points to
        """
        return self.cur_offset

    def _get_range(self, key: str) -> Tuple[int, int]:
        """
        :param key: the key user intends to find
        :return: the index range for key according
        to the index table in memory
        """
        start = -1
        end = -1
        index_table_len = len(self.index_table)
        left = 0
        right = index_table_len - 1
        while left <= right and right >= 0 and left < index_table_len:
            mid = int((left + right) / 2)
            cur_data = self.index_table[mid]
            if cur_data[0] == key:
                start = unpack("i", cur_data[1])[0]
                end = unpack("i", cur_data[1])[0]
                return start, end
            elif cur_data[0] < key:
                left = mid + 1
                start = unpack("i", cur_data[1])[0]
            elif cur_data[0] > key:
                right = mid - 1
                end = unpack("i", cur_data[1])[0]
        return start, end

    # _get_offset: find the offset of key in index file
    def _get_offset(self, key: str, start, end):
        """
        :param key: the key user intends to find in sstable
        :param start: the start of the index range
        :param end: the end of the index range
        :return: the offset of the key;
        if the offset is not found, return None
        """
        if start == -1:
            start = 0
        if end == -1:
            end = self.last_index
        self.index_file.seek(start)
        bin_key = key.encode()
        while start <= end:
            key_size_str = self.index_file.read(self.INT_SIZE)
            key_size = unpack("i", key_size_str)[0]
            cur_key = self.index_file.read(key_size)
            if cur_key == bin_key:
                offset_str = self.index_file.read(self.INT_SIZE)
                offset = unpack("i", offset_str)[0]
                return offset
            else:
                self.index_file.read(self.INT_SIZE)
            start += self.INT_SIZE + key_size + self.INT_SIZE
        return None
