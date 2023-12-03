#!/usr/bin/env python3
import sys

# Pass the type X as a command-line argument
type_x = sys.argv[1]

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')

    # Check if the line is a header or if it has enough parts
    if len(parts) > 10 and parts[0] != 'X':
        interconne = parts[10]
        detection = parts[9]

        # Output the line if Interconne matches type X
        if interconne == type_x:
            print(f'{detection}\t1')