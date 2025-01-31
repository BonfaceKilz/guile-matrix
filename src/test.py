import lmdb
import sys
import json

env = lmdb.open(f'/export5/lmdb-data/{sys.argv[1]}-{sys.argv[2]}', max_dbs=2)

with env.begin(write=True) as txn:
    data = {}
    with env.begin() as txn:
        for x, _ in txn.cursor():
            value = float(txn.get(x).decode())
            data[x.decode()] = value
    print(json.dumps(data))
