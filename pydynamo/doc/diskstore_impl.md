Diskstore combines the memory storage and sstables. Operations on data will be stored in memory table until the size of
expected sstable file reaches a configured threshold. Then the memory table will be flushed into disk. A index table will
be kept in memory to track partial data's key and offset of the data in index file.

1.  Store Directory:
    path/store name
    SStable Directory:
    path/store name/sstable/
    Index file Directory:
    path/store name/index/

2.  File names:
    Index file is named by: store name + id + ".index"
    SStable file is named by: store name + id + ".ss"

3.  Table format:
    a.  Index table: It is a list of tuple.
        (key, offset of the key in index file)
        Every sstable has a in memory index table to track 

    b.  Index file format:
        A data block contains:
        [key size] (4 bytes): size of key
        [key     ] (key size): key
        [Offset] ( 4 bytes): the offset  of the key in sstable file

    c.  SSTable file format:
        The indicator shows whether it is a removed. if it is 0, then the data is removed. Otherwise, the data is inserted or updated.

        if user operates delete on this data, the data block format is:
        [INDICATOR] (4 bytes): 0
        [Key size ] (4 bytes): an integer to indicate size of key
        [key]       (key size): key
        [Timestamp] (8 bytes): the time of the operation, used to track whether it is latest operation

        If user operates other operation on the data, the data block format is:
        [INDICATOR] (4 bytes): 1
        [Key size ] (4 bytes): an integer to indicate size of key
        [key]       (key size): key
        [Val size ] (4 bytes): an integer to indicate size of value
        [Value]     (Val size)
        [Timestamp] (8 bytes): the time of the operation, used to track whether it is latest operation
elo
4.  APIs:
    a.  get(key):
        It is used to get the value corresponding to the key.
        It will check memory table first. If this key exist in this memory table, then return the corresponding value.
        If this key has been operated delete operation, then return None.

    b.  set(key, value):
        It is used to update a value corresponding to the key.
        It will be updated into memory table.
        If the expected size of sstable file reaches the threshold,
        flush the data into a sstable.

    c.  remove(key):
        It is used to remove a key-value set.
        It will be operated in memory stable.

    d.  iterator():
        flush current data in memory table to disk.
        merge all sstables.
        
    e.  merge()
        Traverse all the sstables to merge them into one sstable.
        Use self.ss_tables to store all sstables created.
        Store all data into memory, merge them and store them into new sstable.
        clear all other ss_tables(delete index, sstable files, clean self.sstables)
        append new sstable into self.sstables.
        

    

