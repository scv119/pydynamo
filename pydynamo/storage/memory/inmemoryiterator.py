from .binarysearchtree import BinarySearchTree
from ..error import StorageException
from ..error import ErrorType
from typing import Union
from .treenode import TreeNode
from ..iterator import Iterator


class InMemoryIterator(Iterator):
    def __init__(self, database: BinarySearchTree) -> None:
        self.database = database
        self.cur: Union[None, TreeNode] = None
        self.start = True

    def seek(self, key: str) -> None:
        if self.database.contain(key):
            self.cur = self.database.get_node(key)
        else:
            self.seek_to_first()
            raise StorageException(ErrorType.NOT_FOUND,
                                   "This key cannot be found in store.")

    def seek_to_first(self) -> None:
        self.cur = None
        self.start = True

    def valid(self) -> bool:
        if self.database.find_min() is None:
            return False
        elif self.cur is None and self.start:
            return True
        elif self.database.find_next(self.cur):
            return True
        return False

    def next(self) -> None:
        if self.cur is None and self.start:
            self.cur = self.database.find_min()
        else:
            self.cur = self.database.find_next(self.cur)
        self.start = False

    def value(self) -> str:
        if self.cur:
            return self.cur.value
        else:
            raise StorageException(ErrorType.NONE_POINTER,
                                   "This object is None and "
                                   "it has no attribute value.")

    def key(self) -> str:
        if self.cur:
            return self.cur.key
        else:
            raise StorageException(ErrorType.NONE_POINTER,
                                   "This object is None and "
                                   "it has no attribute key.")

    def is_removed(self) -> bool:
        if self.cur and self.cur.remove:
            return True
        return False
