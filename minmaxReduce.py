#!/usr/bin/env python3
import sys
from operator import itemgetter

table = {}
minmax = int(sys.argv[1])

for line in sys.stdin:
    d_col, minmax_col = line.split("\0")

    if d_col in table:
        try:
            if minmax == 0:
                if float(minmax_col) < table[d_col]:
                    table[d_col] = float(minmax_col)
            else:
                if float(minmax_col) > table[d_col]:
                    table[d_col] = float(minmax_col)
        except:
            pass
    else:
        try:
            table[d_col] = float(minmax_col)
        except:
            pass

for key, value in table.items():
    print(key, value)