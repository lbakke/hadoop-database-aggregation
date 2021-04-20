#!/usr/bin/env python3
import sys
from operator import itemgetter

table = {}

for line in sys.stdin:
    d_col, avg_col = line.split("\0")

    if d_col in table:
        try:
            temp = table[d_col][0] * table[d_col][1]
            temp += int(avg_col)
            table[d_col][1] += 1
            table[d_col][0] = temp/table[d_col][1]
        except:
            pass
    else:
        try:
            table[d_col] = [int(avg_col), 1]
        except:
            pass

for key, value in table.items():
    print(key, value[0])