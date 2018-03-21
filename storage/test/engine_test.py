import unittest
from storage.engine import StorageEngine


class StorageEngineTest(unittest.TestCase):
    def test_smoke(self) -> None:
        engine = StorageEngine()
        self.assertTrue(isinstance(engine, StorageEngine))