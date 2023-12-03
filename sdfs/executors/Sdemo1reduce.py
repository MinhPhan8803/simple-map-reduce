#!/usr/bin/env python3
import sys

def reduce_detections(input_files, destination_file):
    detection_count = {}
    total_count = 0

    # Read each input file
    for input_file in input_files:
        with open('/home/sdfs/mrin/' + input_file, 'r') as file:
            lines = file.readlines()

            for line in lines:
                detection, count = line.strip().split('\t')
                count = int(count)

                if detection in detection_count:
                    detection_count[detection] += count
                else:
                    detection_count[detection] = count

                total_count += count

    # Write the result to the destination file
    with open('/home/sdfs/mrout/' + destination_file, 'w') as file:
        for detection, count in detection_count.items():
            percent = (count / total_count) * 100
            file.write(f'{detection}\t{percent:.2f}%\n')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python reduce.py <destination_file> [input_files]")
        sys.exit(1)

    destination_file = sys.argv[1]
    input_files = sys.argv[2:]

    reduce_detections(input_files, destination_file)
