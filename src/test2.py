"""LMD"""
import sys
import json
import hashlib

import lmdb


dataset_name, trait_id = sys.argv[1:3]
checksum = hashlib.md5(f"{dataset_name}-{trait_id}".encode()).hexdigest()
env = lmdb.open(f'/export5/lmdb-data-hashes/{checksum}', max_dbs=15)

with env.begin(write=False) as txn:
    data = {}
    with env.begin() as txn:
        for x, _ in txn.cursor():
            value = float(txn.get(x).decode())
            data[x.decode()] = value
    print(json.dumps(data))
