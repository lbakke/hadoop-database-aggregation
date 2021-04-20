#!/usr/bin/env python3
import sys
from operator import itemgetter

table = {}

for line in sys.stdin:
    d_col, sum_col = line.split("\0")

    if d_col in table:
        try:
            table[d_col] += int(sum_col)
        except:
            pass
    else:
        try:
            table[d_col] = int(sum_col)
        except:
            pass

for key, value in table.items():
    print(key, value)