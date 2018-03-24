from .binarysearchtree import BinarySearchTree


class Iterator(object):
    def __init__(self, database: BinarySearchTree):
        self.database = database
        self.cur = None
        self.start = True

    def seek(self, key: str) -> None:
        if self.database.contain(key):
            self.cur = self.database.get_node(key)
        else:
            raise Exception
            print("This key does not exist in this database. Iterator back to the beginning.")
            self.seek_to_first()

    def seek_to_first(self) -> None:
        self.cur = None
        self.start = True

    def valid(self) -> bool:
        if self.cur is None and self.start:
            return 1
        elif self.database.find_next(self.cur):
            return 1
        return 0

    def next(self) -> None:
        if not self.cur and self.start:
            self.cur = self.database.find_min()
        else:
            self.cur = self.database.find_next(self.cur)
        self.start = False

    def value(self) -> str:
        return self.cur.value

    def key(self) -> str:
        return self.cur.key