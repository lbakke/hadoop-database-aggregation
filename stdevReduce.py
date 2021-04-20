#!/usr/bin/env python3
import sys
from operator import itemgetter

table = {}

for line in sys.stdin:
    d_col, stdev_col = line.split("\0")

    if d_col in table:
        try:
            temp = table[d_col][0] * table[d_col][1]
            temp += float(stdev_col)
            table[d_col][1] += 1
            new_avg = temp/table[d_col][1]
            old_stdev = pow(table[d_col][2], 2)
            table[d_col][2] = pow((old_stdev + (((float(stdev_col) - table[d_col][0]) * (float(stdev_col) - new_avg) - old_stdev) / table[d_col][1])), .5)
            table[d_col][0] = new_avg
        except:
            pass
    else:
        try:
            table[d_col] = [float(stdev_col), 1, 0]
        except:
            pass

for key, value in table.items():
    print(key, value)