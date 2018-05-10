from .treenode import TreeNode
from ..error import StorageException, ErrorType


class BinarySearchTree(object):
    def __init__(self):
        self.root = None
        self.size = 0

    def length(self):
        return self.size

    def insert(self, key: str, value: str) -> None:
        self.root = self._insert_at(key, value, self.root)
        self.size += 1

    def _insert_at(self, key: str, value: str, cur_node: TreeNode) -> TreeNode:
        if not cur_node:
            return TreeNode(key, value)
        elif key < cur_node.key:
            cur_node.left = self._insert_at(key, value, cur_node.left)
        else:
            cur_node.right = self._insert_at(key, value, cur_node.right)
        return cur_node

    def contain(self, key: str) -> bool:
        cur = self.root
        while cur:
            if cur.key == key:
                return True
            elif cur.key < key:
                cur = cur.right
            else:
                cur = cur.left
        return False

    def get(self, key: str) -> str:
        cur = self.root
        while cur:
            if cur.key == key:
                return cur.value
            elif cur.key < key:
                cur = cur.right
            else:
                cur = cur.left
        return None

    def contain_not_removed(self, key: str) -> bool:
        cur = self.root
        while cur:
            if cur.key == key:
                if not cur.remove:
                    return True
                else:
                    return False
            elif cur.key < key:
                cur = cur.right
            else:
                cur = cur.left
        return False

    def get_node(self, key: str) -> TreeNode:
        cur = self.root
        while cur:
            if cur.key == key:
                return cur
            elif cur.key < key:
                cur = cur.right
            else:
                cur = cur.left
        return None

    def update(self, key: str, value: str) -> None:
        cur = self.root
        while cur:
            if cur.key == key:
                cur.value = value
                if cur.remove:
                    cur.remove = False
                return
            elif cur.key < key:
                cur = cur.right
            else:
                cur = cur.left

    def remove(self, key: str) -> None:
        if self.contain(key):
            node = self.get_node(key)
            node.remove = True
        else:
            self.insert(key, None)
            node = self.get_node(key)
            node.remove = True

    def balance_tree(self) -> None:
        # TODO
        pass

    def remove_node(self, key: str) -> None:
        if self.contain(key):
            self.root = self._remove_node_at(key, self.root)
        else:
            raise StorageException(ErrorType.NOT_FOUND,
                                   "This key is not in table.")

    def _remove_node_at(self, key: str, root: TreeNode) -> TreeNode:
        if root.key == key:
            self.size -= 1
            if root.right is None:
                return root.left
            else:
                leftmost_node = self._get_leftmost_node(root.right)
                root.key = leftmost_node.key
                root.value = leftmost_node.value
                root.right = self._remove_left_most_child(root.right)
        elif key < root.key:
            root.left = self._remove_node_at(key, root.left)
        elif key > root.key:
            root.right = self._remove_node_at(key, root.right)
        return root

    def _get_leftmost_node(self, cur: TreeNode) -> TreeNode:
        leftmost = cur
        if cur is None:
            return None
        while cur.left is not None:
            cur = cur.left
            leftmost = cur
        return leftmost

    def _remove_left_most_child(self, cur: TreeNode) -> TreeNode:
        right_child = cur.right
        if not cur.left:
            return right_child
        else:
            cur.left = self._remove_left_most_child(cur.left)
        return cur

    def find_next(self, cur) -> TreeNode:
        return self._find_next(cur, self.root)

    def _find_next(self, cur_node, cur_root) -> TreeNode:
        if not cur_root or not cur_node:
            return None
        if cur_node.key >= cur_root.key:
            return self._find_next(cur_node, cur_root.right)
        else:
            left = self._find_next(cur_node, cur_root.left)
            if left:
                return left
            else:
                return cur_root

    def find_min(self) -> TreeNode:
        return self._get_leftmost_node(self.root)
