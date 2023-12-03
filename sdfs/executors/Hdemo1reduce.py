#!/usr/bin/env python3
import sys

current_detection = None
current_count = 0
detection_count = {}

for line in sys.stdin:
    line = line.strip()
    detection, count = line.split('\t', 1)
    count = int(count)

    if current_detection == detection:
        current_count += count
    else:
        if current_detection:
            # Store the count for the current detection type
            detection_count[current_detection] = current_count
        current_detection = detection
        current_count = count

# Don't forget to output the last detection type
if current_detection == detection:
    detection_count[current_detection] = current_count

total = sum(detection_count.values())
for detection, count in detection_count.items():
    percent = (count / total) * 100
    print(f'{detection}\t{percent:.2f}%')