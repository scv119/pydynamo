Index file format:
A data block contains:
[key size] (4 bytes): size of key
[key     ] (key size): key
[Offset] ( 4 bytes): the offset  of the key in sstable file

SSTable file format:
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



