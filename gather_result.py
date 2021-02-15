#!/usr/bin/env python3

import re

WORKER_COUNT = 20
REGEX = re.compile(r'^idx=(\d+) \(out of (\d+)\)$')

total = 0
finished = 0

for i in range(WORKER_COUNT):
    with open(f'progress_log{i}.txt', 'r') as log_file:
        stat = log_file.read().rsplit('\n\n', maxsplit=2)[-2].split('\n')[0]
        print(f'worker#{i} {stat}')
        matched = REGEX.search(stat)
        finished += int(matched.group(1))
        total += int(matched.group(2))

print(f'{finished} out of {total} in total')
