import unittest
from pydynamo.storage.memory.binarysearchtree import BinarySearchTree


class BinarySearchTreeTest(unittest.TestCase):
    def setUp(self):
        self.bst = BinarySearchTree()
        self.bst.insert("2", "abandon")
        self.bst.insert("1", "definition")
        self.bst.insert("4", "support")
        self.bst.insert("3", "aggressive")

    def test_remove(self) -> None:
        self.assertEqual(self.bst.get("1"), "definition")
        self.bst.remove_node("1")
        self.assertTrue(not self.bst.contain("1"))
        self.assertEqual(self.bst.get("2"), "abandon")
        self.bst.remove_node("2")
        self.assertTrue(not self.bst.contain("2"))
        self.assertEqual(self.bst.get("4"), "support")
        self.bst.remove_node("4")
        self.assertTrue(not self.bst.contain("4"))
        self.assertEqual(self.bst.get("3"), "aggressive")
        self.bst.remove_node("3")
        self.assertTrue(not self.bst.contain("3"))

    def test_insert(self) -> None:
        self.bst.insert("3.5", "test")
        self.assertEqual(self.bst.get("3.5"), "test")
        self.bst.insert("-1", "test1")
        self.assertEqual(self.bst.get("-1"), "test1")

    def test_get_contain(self) -> None:
        self.assertTrue(self.bst.contain("1"))
        self.assertTrue(self.bst.contain("2"))
        self.assertTrue(self.bst.contain("3"))
        self.assertTrue(self.bst.contain("4"))
        self.assertTrue(not self.bst.contain("5"))
        self.assertEqual(self.bst.get("1"), "definition")
        self.assertEqual(self.bst.get("2"), "abandon")
        self.assertEqual(self.bst.get("3"), "aggressive")
        self.assertEqual(self.bst.get("4"), "support")
        self.assertEqual(self.bst.get("5"), None)

    def test_update(self) -> None:
        self.bst.update("1", "test")
        self.assertEqual(self.bst.get("1"), "test")
        self.bst.update("2", "test2")
        self.assertEqual(self.bst.get("2"), "test2")
        self.bst.update("3", "test3")
        self.assertEqual(self.bst.get("3"), "test3")
