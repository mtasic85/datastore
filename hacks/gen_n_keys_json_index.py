# usage:
#   python -B gen_n_keys_json_index.py N_KEYS KEY_LEN
# example:
#   python -B gen_n_keys_json_index.py 1000000 3

from random import randint
import os
import sys
import json

N = int(sys.argv[1])
M = int(sys.argv[2])
filename = 'index.json'

types = (int, float, str)
items = []

for i in range(N):
    key = []

    for j in range(M):
        f = types[j % 3]
        k = f(randint(0, 2 ** 32))
        key.append(k)

    key = tuple(key)
    value = randint(0, 2 ** 64)
    item = (key, value)
    items.append(item)

items.sort(key=lambda n: n[0])

with open(filename, 'w') as f:
    for item in items:
        _item = json.dumps(item)
        f.write(_item)
        f.write('\n')
