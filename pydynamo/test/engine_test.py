import unittest
from pydynamo.storage.inmemorystore import InMemoryStore
from pydynamo.storage.inmemoryengine import InMemoryStorageEngine

class InMemoryStorageEngineTest(unittest.TestCase):
    def test_smoke(self) -> None:
        engine = InMemoryStorageEngine()
        self.assertTrue(isinstance(engine, InMemoryStorageEngine))

    def test_creat_store(self):
        new_store = InMemoryStorageEngine().create_store("test")
        self.assertTrue(isinstance(new_store, InMemoryStore))

    def test_get_store(self):
        #new_store = StorageEngine().create_store("test")
        #self.assertEqual(StorageEngine().get_store("test"), new_store)
        pass

