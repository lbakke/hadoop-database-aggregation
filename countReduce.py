#!/usr/bin/env python3
import sys
from operator import itemgetter

table = {}

for line in sys.stdin:
    (name,one) = line.split("\0")

    if name in table:
        table[name] += 1
    else:
        table[name] = 1

for key, value in table.items():
    print(key, value)