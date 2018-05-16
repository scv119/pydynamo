The sstables are maintained by storing them into disk files. For each sstable, there should be two corresponding files.
One is index file that stores key and the offset of each key. The other one is sstable file that stores the key, value,
timestamp and indicator. SStables are kept as static files. So remove, set or any operation that tries to modify this
table should be forbidden.

sstable APIs:
1.  get(key):
    It is used to get the value corresponding to key.
    If this key does not exist, then raise an NOT_FOUND exception.
    If this key is operated by a delete operation, then raise an NOT_FOUND exception.

2.  get_timestamp(key):
    It is used to get the timestamp of the key in this sstable.

3.  contain(key):
    It is used to check whether one key exist in the sstable.
    Deleted data set is considered to be contained in sstables.

4.  remove(key), set(key, value):
    It is forbidden to modify the sstable.

sstable Iterator:
1.  seek(key):
    It is used to move to offset of this key.

2.  seek_to_first():
    It is used to move to the beginning dummy node.

3.  valid():
    It is used to track whether there is a next data set. Removed key node is considered as a valid node.

4.  next():
    It is used to point to offset of next data set. Removed key-value node can be next node.

5.  value():
    It is used to return the value of current data. If this data is removed, then raise an NOT_FOUND exception.

6.  key():
    It is used to return the key of current data set.

7.  timestamp():
    It is used to return the timestamp of current data set.

8.  length():
    It is used to return the length of current data set.

9.  is_removed():
    It is used to track whether current data set has been operated a delete operation.