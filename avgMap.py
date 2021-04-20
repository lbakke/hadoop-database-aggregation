#!/usr/bin/env python3

import sys
import io


stream = io.TextIOWrapper(sys.stdin.buffer, encoding='iso-8859-1')

d_col = int(sys.argv[1])
avg_col = int(sys.argv[2])
for count, line in enumerate(stream):
    if count == 0:
        continue
    row_entry = line.split(",")
    
    print(str(row_entry[d_col]) + "\0" + str(row_entry[avg_col]))