#!/usr/bin/env python3

import sys
import io


stream = io.TextIOWrapper(sys.stdin.buffer, encoding='iso-8859-1')

d_col = int(sys.argv[1])
for count, line in enumerate(stream):
    if count == 0:
        continue
    row_entry = line.split(",")
    row_entry[0] = row_entry[0][1:-1]
    
    print(row_entry[d_col], 1)
