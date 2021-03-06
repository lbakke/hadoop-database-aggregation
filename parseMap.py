#!/usr/bin/env python3

import sys
import io


stream = io.TextIOWrapper(sys.stdin.buffer, encoding='iso-8859-1')

for line in stream:
    row_entry = line.split(",")
    if row_entry[0][0] == " ":
        row_entry[0] = row_entry[0][1:]
    if row_entry[0][-1] == " ":
        row_entry[0] = row_entry[0][:-1]
    row_entry = row_entry[:-1]
    new_string = ""
    for count, term in enumerate(row_entry):
        if count == 0:
            new_string = str(term)
        else:
            new_string += "," + str(term)
    print(new_string)