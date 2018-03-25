from typing import Any


class StorageEngine(object):

    def create_store(self, store_name: str) -> Any:
        raise Exception("not implemented")

    def get_store(self, store_name: str) -> Any:
        raise Exception("not implemented")
