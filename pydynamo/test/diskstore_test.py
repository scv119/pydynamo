import unittest
from ..storage.disk.diskstore import DiskStore
import tempfile


class DiskStoreTest(unittest.TestCase):
    def setUp(self):
        """
        create a sstable stored in temp directory
        :return:
        """
        with tempfile.TemporaryDirectory() as tempdirname:
            self.disk_2000 = DiskStore("test1", tempdirname, 2000)
            self.disk_200 = DiskStore("test2", tempdirname, 200)
            self.disk_20 = DiskStore("test3", tempdirname, 20)
            self.dis = DiskStore("test4", tempdirname, 10)
            for i in range(100):
                self.disk_2000.set(str(i), "result" + str(i))
                self.disk_200.set(str(i), "result" + str(i))
                self.disk_20.set(str(i), "result" + str(i))
            for i in range(50):
                self.disk_2000.set(str(i), "new" + str(i))
                self.disk_200.set(str(i), "new" + str(i))
                self.disk_20.set(str(i), "new" + str(i))

    def test_get(self):
        for i in range(50):
            self.assertEqual(self.disk_2000.get(str(i)), "new" + str(i))
            self.assertEqual(self.disk_200.get(str(i)), "new" + str(i))
            self.assertEqual(self.disk_20.get(str(i)), "new" + str(i))
        for i in range(51, 100):
            self.assertEqual(self.disk_2000.get(str(i)), "result" + str(i))
            self.assertEqual(self.disk_200.get(str(i)), "result" + str(i))
            self.assertEqual(self.disk_20.get(str(i)), "result" + str(i))

    def test_set(self):
        print("TEST SET")
        with tempfile.TemporaryDirectory() as tempdirname:
            disk = DiskStore("temp", tempdirname, 1000)
            for i in range(100):
                disk.set(str(i), "result" + str(i))
            for i in range(100):
                self.assertEqual(disk.get(str(i)), "result" + str(i))
            for i in range(100):
                disk.set(str(i), "new" + str(i))
            for i in range(100):
                self.assertEqual(disk.get(str(i)), "new" + str(i))
        with tempfile.TemporaryDirectory() as tempdirname:
            disk = DiskStore("temp", tempdirname, 200)
            for i in range(100):
                disk.set(str(i), "result" + str(i))
            for i in range(100):
                self.assertEqual(disk.get(str(i)), "result" + str(i))
            for i in range(100):
                disk.set(str(i), "new" + str(i))
            for i in range(100):
                self.assertEqual(disk.get(str(i)), "new" + str(i))
        with tempfile.TemporaryDirectory() as tempdirname:
            disk = DiskStore("temp", tempdirname, 20)
            for i in range(100):
                disk.set(str(i), "result" + str(i))
            for i in range(100):
                self.assertEqual(disk.get(str(i)), "result" + str(i))
            for i in range(100):
                disk.set(str(i), "new" + str(i))
            for i in range(100):
                self.assertEqual(disk.get(str(i)), "new" + str(i))
