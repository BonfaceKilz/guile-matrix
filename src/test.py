import lmdb
import struct

env = lmdb.open('/tmp/test', max_dbs=2)
with env.begin(write=True) as txn:
    val = txn.get("10007".encode())
    print(tuple(map(lambda x: round(x, 6),
                    struct.unpack("<"+"f"*(len(val)//4), val))))
