from .engine import StorageEngine
from .inmemorystore import InMemoryStore
from .error import StorageException
from .error import ErrorType


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
            raise StorageException(ErrorType.NOT_FOUND,
                                   "This store cannot be found.")
