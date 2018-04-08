from ..error import StorageException
from ..error import ErrorType
from ..iterator import Iterator
from struct import unpack, pack
import os


class SStableIterator(Iterator):
    INT_SIZE = 4

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
        self.cur_offset = -1
        self.file_size = size
        self.last_index = last_index

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
        index_range = self._get_range(key)
        if len(index_range) == 1:
            offset = unpack("i", index_range[0])[0]
            self.cur_offset = offset
        elif len(index_range) == 2:
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
        self.cur_offset = -1

    def valid(self) -> bool:
        """
        :return: true if the iterator has next. Otherwise false
        """
        if self.cur_offset == -1:
            return True
        elif self.cur_offset < -1:
            return False
        else:
            self.sstable_file.seek(self.cur_offset)
            key_size_str = self.sstable_file.read(self.INT_SIZE)
            key_size = unpack("i", key_size_str)[0]
            self.sstable_file.read(key_size)
            val_size_str = self.sstable_file.read(self.INT_SIZE)
            val_size = unpack("i", val_size_str)[0]
            self.sstable_file.read(val_size)
            time_size_str = self.sstable_file.read(self.INT_SIZE)
            time_size = unpack("i", time_size_str)[0]
            offset = self.cur_offset + self.INT_SIZE \
                + key_size + self.INT_SIZE + val_size + \
                self.INT_SIZE + time_size
            if offset >= self.file_size:
                return False
            return True

    def next(self) -> None:
        """
        if iterator has next, set the iterator points to next;
        otherwise, raise an exception
        :return: None
        """
        if self.cur_offset == -1:
            self.cur_offset = 0
        elif self.cur_offset >= 0:
            self.sstable_file.seek(self.cur_offset)
            key_size_str = self.sstable_file.read(self.INT_SIZE)
            key_size = unpack("i", key_size_str)[0]
            self.sstable_file.read(key_size)
            val_size_str = self.sstable_file.read(self.INT_SIZE)
            val_size = unpack("i", val_size_str)[0]
            self.sstable_file.read(val_size)
            time_size_str = self.sstable_file.read(self.INT_SIZE)
            time_size = unpack("i", time_size_str)[0]
            self.cur_offset = self.cur_offset + self.INT_SIZE + key_size \
                + self.INT_SIZE + val_size + self.INT_SIZE + time_size
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
        time_size_str = self.sstable_file.read(self.INT_SIZE)
        time_size = unpack("i", time_size_str)[0]
        time_stamp = self.sstable_file.read(time_size).decode()
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
        self.sstable_file.read(val_size)
        time_size_str = self.sstable_file.read(self.INT_SIZE)
        time_size = unpack("i", time_size_str)[0]
        self.sstable_file.read(time_size)
        length = self.INT_SIZE + key_size \
            + self.INT_SIZE + val_size \
            + self.INT_SIZE + time_size
        return length

    def get_cur_offset(self) -> int:
        """
        :return: the offset of data the iterator currently points to
        """
        return self.cur_offset

    def _get_range(self, key: str):
        """
        :param key: the key user intends to find
        :return: the index range for key according
        to the index table in memory
        """
        start = pack("i", -1)
        end = pack("i", -1)
        for i in self.index_table:
            cur_key = i[0]
            if cur_key == key:
                return [i[1]]
            elif cur_key < key:
                start = i[1]
            elif cur_key > key:
                end = i[1]
                break
        return [start, end]

    # _get_offset: find the offset of key in index file
    def _get_offset(self, key: str, start, end):
        """
        :param key: the key user intends to find in sstable
        :param start: the start of the index range
        :param end: the end of the index range
        :return: the offset of the key;
        if the offset is not found, return None
        """
        start_offset = unpack("i", start)[0]
        end_offset = unpack("i", end)[0]
        if start_offset == -1:
            start_offset = 0
        if end_offset == -1:
            end_offset = self.last_index
        self.index_file.seek(start_offset)
        cur_offset = start_offset
        bin_key = key.encode("ascii")
        while cur_offset <= end_offset:
            key_size_str = self.index_file.read(self.INT_SIZE)
            key_size = unpack("i", key_size_str)[0]
            cur_key = self.index_file.read(key_size)
            if cur_key == bin_key:
                offset_str = self.index_file.read(self.INT_SIZE)
                offset = unpack("i", offset_str)[0]
                return offset
            else:
                self.index_file.read(self.INT_SIZE)
            cur_offset += self.INT_SIZE + key_size + self.INT_SIZE
        return None
