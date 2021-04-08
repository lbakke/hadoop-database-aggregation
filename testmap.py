#!/usr/bin/env python3

import sys
import io

stream = io.TextIOWrapper(sys.stdin.buffer, encoding='iso-8859-1')

for line in stream:
    line = line.strip()
    words = line.split()
    for word in words:
        print(f'{word},1')