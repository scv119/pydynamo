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
            disk = DiskStore("test", tempdirname)
            disk.mem_table.set("mengying", "computerscience")
            disk.mem_table.set("kelly", "kpmg")
            disk.mem_table.set("chen", "facebook")
            disk.mem_table.set("jean", "deloitte")
            self.sstable = disk.create_sstable()
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
        self.assertTrue(isinstance(self.sstable.diskIterator, SStableIterator))

    def test_iterator_seek(self) -> None:
        """
        it is used to test iterator's seek function
        :return:
        """
        iterator = self.sstable.diskIterator
        iterator.seek("kelly")
        self.assertEqual(iterator.value(), "kpmg")
        iterator.seek("jean")
        self.assertEqual(iterator.value(), "deloitte")
        iterator.seek("chen")
        self.assertEqual(iterator.value(), "facebook")
        iterator.seek("mengying")
        self.assertEqual(iterator.value(), "computerscience")

    def test_iterator_seek_to_first(self) -> None:
        """
        it is used to test seek_to_first function
        :return:
        """
        iterator = self.sstable.diskIterator
        iterator.seek("kelly")
        iterator.seek_to_first()
        iterator.next()
        self.assertEqual(iterator.key(), "chen")
        self.assertEqual(iterator.value(), "facebook")

    def test_iterator_next_key_value(self) -> None:
        """
        it is used to test iterator's next, key, value functions
        :return:
        """
        iterator = self.sstable.diskIterator
        iterator.next()
        self.assertEqual(iterator.key(), "chen")
        self.assertEqual(iterator.value(), "facebook")
        iterator.next()
        self.assertEqual(iterator.key(), "jean")
        self.assertEqual(iterator.value(), "deloitte")
        iterator.next()
        self.assertEqual(iterator.key(), "kelly")
        self.assertEqual(iterator.value(), "kpmg")
        iterator.next()
        self.assertEqual(iterator.key(), "mengying")
        self.assertEqual(iterator.value(), "computerscience")
        with self.assertRaises(StorageException):
            iterator.next()

    def test_iterator_length(self) -> None:
        """
        it is for testing iterator length function
        :return:
        """
        iterator = self.sstable.diskIterator
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
        iterator = self.sstable.diskIterator
        self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(iterator.valid())
        iterator.next()
        self.assertTrue(not iterator.valid())
