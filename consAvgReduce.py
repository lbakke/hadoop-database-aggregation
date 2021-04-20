#!/usr/bin/env python3
import sys
from operator import itemgetter


order = int(sys.argv[1])
sort_type = int(sys.argv[2])
limit = int(sys.argv[3])
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

if order == 0:
    count = 0
    if sort_type == 0:
        for key, value in sorted(table.items(), key=itemgetter(0), reverse=False):
            if limit < 0 or count < limit:
                print(key, '%.2f' % value[0])
                count += 1
    else:
        for key, value in sorted(table.items(), key=itemgetter(0), reverse=True):
            if limit < 0 or count < limit:
                print(key, '%.2f' % value[0])
                count += 1
else:
    count = 0
    if sort_type == 0:
        for key, value in sorted(table.items(), key=itemgetter(1), reverse=False):
            if limit < 0 or count < limit:
                print(key, '%.2f' % value[0])
                count += 1
    else:
        for key, value in sorted(table.items(), key=itemgetter(1), reverse=True):
            if limit < 0 or count < limit:
                print(key, '%.2f' % value[0])
                count += 1