#!/usr/bin/env python3

import sys
import io


stream = io.TextIOWrapper(sys.stdin.buffer, encoding='iso-8859-1')

for line in stream:
    row_entry = line.split(" ")
    length = len(row_entry)
    if length == 2:
        print(str(row_entry[0]) + "\0" + str(row_entry[1]))
    elif length == 3:
        print(str(row_entry[0]) + " " + str(row_entry[1]) + "\0" + str(row_entry[2]))
    else:
        pass