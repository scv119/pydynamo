import unittest
from pydynamo.storage.inmemorystore import InMemoryStore

test_cases = [["2", "abandon"], ["1", "definition"], ["3", "support"]]


class StoreTest(unittest.TestCase):
    def test_smoke(self) -> None:
        store = InMemoryStore("test")
        self.assertTrue(isinstance(store, InMemoryStore))

    def test_add_get_method(self) -> None:
        store = self._generate_store(test_cases)
        self.assertEqual(store.get("2"), "abandon")
        self.assertEqual(store.get("1"), "definition")
        self.assertEqual(store.get("3"), "support")

    def test_update(self) -> None:
        store = self._generate_store(test_cases)
        store.set("1", "update")
        self.assertEqual(store.get("1"), "update")

    def test_get(self) -> None:
        store = self._generate_store(test_cases)
        self.assertEqual(store.get("4"), None)

    def _generate_store(self, list)-> InMemoryStore:
        store = InMemoryStore("test")
        for case in list:
            store.set(case[0], case[1])
        return store

    def test_iterator_next(self):
        store = self._generate_store(test_cases)
        iterator = store.iterator()
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
        store = self._generate_store(test_cases)
        iterator = store.iterator()
        self.assertEqual(iterator.valid(), True)
        iterator.next()
        self.assertEqual(iterator.valid(), True)
        iterator.next()
        self.assertEqual(iterator.valid(), True)
        iterator.next()
        self.assertEqual(iterator.valid(), False)

    def test_iterator_seek_to_first(self):
        store = self._generate_store(test_cases)
        iterator = store.iterator()
        iterator.next()
        self.assertEqual(iterator.value(), "definition")
        self.assertEqual(iterator.key(), "1")
        iterator.next()
        self.assertEqual(iterator.value(), "abandon")
        self.assertEqual(iterator.key(), "2")
        iterator.seek_to_first()
        self.assertTrue(iterator.start == True)
        iterator.next()
        self.assertEqual(iterator.value(), "definition")
        self.assertEqual(iterator.key(), "1")

    def test_remove(self):
        store = self._generate_store(test_cases)
        store.remove("2")
        self.assertEqual(store.get("2"), None)
        iterator = store.iterator()
        iterator.next()
        self.assertEqual(iterator.value(), "definition")
        self.assertEqual(iterator.key(), "1")
        iterator.next()
        self.assertEqual(iterator.value(), "support")
        self.assertEqual(iterator.key(), "3")