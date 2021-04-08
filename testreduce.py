#!/usr/bin/env python3

import sys
import io 

stream = io.TextIOWrapper(sys.stdin.buffer, encoding='iso-8859-1')

words = {}

# input comes from STDIN
for line in stream:
    # remove leading and trailing whitespace
    line = line.strip()

    wordsfound = line.split(',')
    word = wordsfound[0]

    if word in words: 
        words[word] = words[word] + 1
    else:
        words[word] = 1

for word in words: 
    print(word, words[word])