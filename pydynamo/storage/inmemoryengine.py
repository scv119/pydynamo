from .engine import StorageEngine
from .inmemorystore import InMemoryStore


class InMemoryStorageEngine(StorageEngine):
    def __init__(self):
        self.stores = {}

    def create_store(self, store_name: str) -> InMemoryStore:
        self.stores[store_name] = InMemoryStore(store_name)
        return self.stores[store_name]

    def get_store(self, store_name: str) -> InMemoryStore:
        if store_name in self.stores:
            return self.stores[store_name]
        else:
            raise Exception
            return None