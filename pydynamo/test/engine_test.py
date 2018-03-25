import unittest
from pydynamo.storage.inmemorystore import InMemoryStore
from pydynamo.storage.inmemoryengine import InMemoryStorageEngine
from pydynamo.storage.error import StorageException


class InMemoryStorageEngineTest(unittest.TestCase):
    def test_smoke(self) -> None:
        engine = InMemoryStorageEngine()
        self.assertTrue(isinstance(engine, InMemoryStorageEngine))

    def test_create_store(self):
        new_store = InMemoryStorageEngine().create_store("test")
        self.assertTrue(isinstance(new_store, InMemoryStore))

    def test_get_store(self):
        engine = InMemoryStorageEngine()
        new_store = engine.create_store("test")
        with self.assertRaises(StorageException):
            engine.get_store("bad")
        self.assertEqual(engine.get_store("test"), new_store)
