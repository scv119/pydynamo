The implementation of memory storage is based on structure of binary search tree.
As removed node should be kept for future merge in sstable files, a boolean variable n tree node is used to indicate
whether this node is removed or not.

TreeNode contains:
key: This is key of the data set.
value: This is the value of the data set.
left: This points to left child node. Its key should be less than current node's key.
right: This points to right child node. Its key should be more than current node's key.
remove: This boolean variable is used to track whether this node is operated by an delete operation. If it is deleted,
this boolean should be True. Else it should be False. It is default to be False.

Inmemory Store APIs:

1.  set(key, value)
    It is used to insert a key-value set into memory table or update a key-value set into memory.

2.  get(key)
    It is used to get value corresponding to key. If this key does not exist or this key-value set is deleted, then raise
    an NOT_FOUND Exception.

3.  remove(key)
    It is used to remove a key. The key-value is still in memory table but its remove variable will be set to True.
    If this key is already deleted, then do nothing.
    If this key does not exist in the table, raise a NOT_FOUND exception.

4.  clean()
    It is used to clean the memory table

5.  get_size()
    It is used to get the size of sstable files. The size could be tracked whether it reaches the threshold.

6.  others:
    contain_key(key): check whether a key exist in memory table. Removed keys are still in table.
    is_removed(key): check one key is operated delete operation.

InMemory Iterator APIs:
1.  seek(key):
    It is used to move to node of this key.

2.  seek_to_first():
    It is used to move to the beginning dummy node.

3.  valid():
    It is used to track whether there is a next node. Removed key-value node is considered as a valid node.

4.  next():
    It is used to point to next node. Removed key-value node can be next node.

5.  value():
    It is used to return the value of current node.

6.  key():
    It is used to return the key of current node.

7.  is_removed():
    It is used to track whether current node has been operated a delete operation.