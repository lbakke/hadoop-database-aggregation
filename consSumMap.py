#!/usr/bin/env python3

import sys
import io


stream = io.TextIOWrapper(sys.stdin.buffer, encoding='iso-8859-1')

for line in stream:
    row_entry = line.strip().split(" ")
    length = len(row_entry)
    if length == 2:
        print(str(row_entry[0]) + "\0" + str(row_entry[1]))
    elif length == 3:
        print(str(row_entry[0]) + " " + str(row_entry[1]) + "\0" + str(row_entry[2]))
    else:
        temp_str = ""
        for count, term in enumerate(row_entry):
            if count != length - 1:
                if count != 0:
                    temp_str += " "
                temp_str += term
        print(temp_str + "\0" + str(row_entry[-1]))