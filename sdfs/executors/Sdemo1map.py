#!/usr/bin/env python3
import sys
import re

def map_interconne(input_file, output_prefix, type_x):
    # Create a dictionary to count occurrences of each detection
    detection_count = {}

    # Open the input files
    with open('/home/sdfs/mrin/' + input_file, mode='r', errors='replace') as file:
        for line in file:
            line = line.strip()
            parts = line.split(',')

            # Check if the line is a header or if it has enough parts
            if len(parts) > 10 and parts[0] != 'X':
                interconne = parts[10]
                detection = parts[9]

                # Process the line if Interconne matches type X
                if interconne == type_x:
                    if detection in detection_count:
                        detection_count[detection] += 1
                    else:
                        detection_count[detection] = 1

    # Create output files for each detection
    for detection, count in detection_count.items():
        with open(f'/home/sdfs/mrout/{output_prefix}_{detection}', 'a') as file:
            file.write(f'{detection}\t{count}\n')
            file.flush()

    for detection in detection_count.keys():
        print(detection)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python map.py <type_x> <output_prefix> [input_files]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_prefix = sys.argv[2]
    type_x = sys.argv[3]

    map_interconne(input_file, output_prefix, type_x)
