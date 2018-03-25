import unittest
from pydynamo.storage.memory.inmemorystore import InMemoryStore
from pydynamo.storage.error import StorageException

test_cases = [["2", "abandon"], ["1", "definition"], ["3", "support"]]


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.store = self._generate_store(test_cases)

    def test_smoke(self) -> None:
        inmemorystore = InMemoryStore("test")
        self.assertTrue(isinstance(inmemorystore, InMemoryStore))

    def test_add_get_method(self) -> None:
        self.assertEqual(self.store.get("2"), "abandon")
        self.assertEqual(self.store.get("1"), "definition")
        self.assertEqual(self.store.get("3"), "support")

    def test_update(self) -> None:
        self.store.set("1", "update")
        self.assertEqual(self.store.get("1"), "update")

    def test_get(self) -> None:
        with self.assertRaises(StorageException):
            self.store.get("4")

    def _generate_store(self, list)-> InMemoryStore:
        store = InMemoryStore("test")
        for case in list:
            store.set(case[0], case[1])
        return store

    def test_iterator_next(self):
        iterator = self.store.iterator()
        iterator.next()
        self.assertEqual(iterator.value(), "definition")
        self.assertEqual(iterator.key(), "1")
        iterator.next()
        self.assertEqual(iterator.value(), "abandon")
        self.assertEqual(iterator.key(), "2")
        iterator.next()
        self.assertEqual(iterator.value(), "support")
        self.assertEqual(iterator.key(), "3")

    def test_iterator_valid(self):
        iterator = self.store.iterator()
        self.assertEqual(iterator.valid(), True)
        iterator.next()
        self.assertEqual(iterator.valid(), True)
        iterator.next()
        self.assertEqual(iterator.valid(), True)
        iterator.next()
        self.assertEqual(iterator.valid(), False)

    def test_iterator_seek_to_first(self):
        iterator = self.store.iterator()
        iterator.next()
        self.assertEqual(iterator.value(), "definition")
        self.assertEqual(iterator.key(), "1")
        iterator.next()
        self.assertEqual(iterator.value(), "abandon")
        self.assertEqual(iterator.key(), "2")
        iterator.seek_to_first()
        self.assertTrue(iterator.start is True)
        iterator.next()
        self.assertEqual(iterator.value(), "definition")
        self.assertEqual(iterator.key(), "1")

    def test_iterator_seek(self):
        iterator = self.store.iterator()
        iterator.seek("1")
        self.assertEqual(iterator.value(), "definition")
        with self.assertRaises(StorageException):
            iterator.seek("4")

    def test_remove(self):
        self.store.remove("2")
        self.assertRaises(StorageException, self.store.get, "2")
        iterator = self.store.iterator()
        iterator.next()
        self.assertEqual(iterator.value(), "definition")
        self.assertEqual(iterator.key(), "1")
        iterator.next()
        self.assertEqual(iterator.value(), "support")
        self.assertEqual(iterator.key(), "3")
