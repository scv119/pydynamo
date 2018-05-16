import unittest
from ..storage.disk.sstable import SSTable
from ..storage.error import StorageException
from ..storage.disk.sstableiterator import SStableIterator
from ..storage.disk.diskstore import DiskStore
import tempfile


class SStableTest(unittest.TestCase):
    def setUp(self):
        """
        create a sstable stored in temp directory
        :return:
        """
        with tempfile.TemporaryDirectory() as tempdirname:
            disk1 = DiskStore("test_empty", tempdirname, 2000000)
            disk1.flush()
            self.sstable1 = disk1.ss_tables[0]
            disk = DiskStore("test", tempdirname, 2000000)
            disk.mem_table.set("mengying", "computerscience")
            disk.mem_table.set("kelly", "kpmg")
            disk.mem_table.set("chen", "facebook")
            disk.mem_table.set("jean", "deloitte")
            for i in range(100):
                disk.mem_table.set(str(i), "result" + str(i))
            self.in_memory_iterator = disk.mem_table.iterator()
            disk.flush()
            self.sstable = disk.ss_tables[0]
            self.tempdir = tempdirname

    def test_smoke(self) -> None:
        self.assertTrue(isinstance(self.sstable, SSTable))

    def test_get(self):
        """
        test get function of sstable
        :return:
        """
        self.assertEqual(self.sstable.get("mengying"), "computerscience")
        self.assertEqual(self.sstable.get("kelly"), "kpmg")
        self.assertEqual(self.sstable.get("chen"), "facebook")
        self.assertEqual(self.sstable.get("jean"), "deloitte")
        for i in range(100):
            self.assertEqual(self.sstable.get(str(i)), "result" + str(i))
        with self.assertRaises(StorageException):
            self.sstable.get("shen")

    def test_set(self):
        """
        test set function of sstable
        :return:
        """
        with self.assertRaises(StorageException):
            self.sstable.set("shen", "zhihu")

    def test_remove(self):
        """
        test remove function of sstable
        :return:
        """
        with self.assertRaises(StorageException):
            self.sstable.remove("kelly")

    def test_iterator(self) -> None:
        """
        test whether created iterator is an instance
        of SStableIterator
        :return:
        """
        self.assertTrue(isinstance(self.sstable.disk_iterator,
                                   SStableIterator))

    def test_iterator_seek(self) -> None:
        """
        it is used to test iterator's seek function
        :return:
        """
        iterator = self.sstable.disk_iterator
        iterator.seek("kelly")
        self.assertEqual(iterator.value(), "kpmg")
        iterator.seek("jean")
        self.assertEqual(iterator.value(), "deloitte")
        iterator.seek("chen")
        self.assertEqual(iterator.value(), "facebook")
        iterator.seek("mengying")
        self.assertEqual(iterator.value(), "computerscience")
        for i in range(100):
            iterator.seek(str(i))
            self.assertEqual(iterator.value(), "result" + str(i))

    def test_iterator_seek_to_first(self) -> None:
        """
        it is used to test seek_to_first function
        :return:
        """
        iterator = self.sstable.disk_iterator
        iterator.seek("kelly")
        iterator.seek_to_first()
        iterator.next()
        self.assertEqual(iterator.key(), "0")
        self.assertEqual(iterator.value(), "result0")

    def test_iterator_next_key_value(self) -> None:
        """
        it is used to test iterator's next, key, value functions
        :return:
        """
        iterator = self.sstable.disk_iterator
        memory_iterator = self.in_memory_iterator
        for i in range(104):
            iterator.next()
            memory_iterator.next()
            self.assertEqual(iterator.key(), memory_iterator.key())
            self.assertEqual(iterator.value(), memory_iterator.value())
        with self.assertRaises(StorageException):
            iterator.next()

    def test_iterator_length(self) -> None:
        """
        it is for testing iterator length function
        :return:
        """
        iterator = self.sstable.disk_iterator
        iterator.next()
        cur = iterator.get_cur_offset()
        length = iterator.length()
        while iterator.valid():
            iterator.next()
            next = iterator.get_cur_offset()
            self.assertEqual(length, next - cur)
            cur = next
            length = iterator.length()

    def test_iterator_valid(self) -> None:
        """
        it is for testing iterator valid function
        :return:
        """
        iterator = self.sstable.disk_iterator
        self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(iterator.valid())
        for i in range(100):
            iterator.next()
            self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(not iterator.valid())

    def test_iterator_empty_sstable(self) -> None:
        iterator = self.sstable1.disk_iterator
        with self.assertRaises(StorageException):
            iterator.seek("test")
        self.assertTrue(not iterator.valid())
