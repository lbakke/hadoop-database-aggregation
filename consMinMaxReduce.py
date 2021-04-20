#!/usr/bin/env python3
import sys
from operator import itemgetter


minmax = int(sys.argv[1])
order = int(sys.argv[2])
sort_type = int(sys.argv[3])
limit = int(sys.argv[4])

table = {}
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

if order == 0:
    count = 0
    if sort_type == 0:
        for key, value in sorted(table.items(), key=itemgetter(0), reverse=False):
            if limit < 0 or count < limit:
                print(key, value[0])
                count += 1
    else:
        for key, value in sorted(table.items(), key=itemgetter(0), reverse=True):
            if limit < 0 or count < limit:
                print(key, value[0])
                count += 1
else:
    count = 0
    if sort_type == 0:
        for key, value in sorted(table.items(), key=itemgetter(1), reverse=False):
            if limit < 0 or count < limit:
                print(key, value[0])
                count += 1
    else:
        for key, value in sorted(table.items(), key=itemgetter(1), reverse=True):
            if limit < 0 or count < limit:
                print(key, value[0])
                count += 1