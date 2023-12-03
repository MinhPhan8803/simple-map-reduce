#!/usr/bin/env python3
import sys
import os

def reduce_detections(input_prefix, destination_file):

    with open('/home/sdfs/mrout/'+ destination_file, 'w') as dest_file:
        detection_count = {}
        total_count = 0
        for filename in os.listdir('/home/sdfs/mrout'):
            if filename.startswith(input_prefix):
                with open(filename, 'r') as file:
                    lines = file.readlines()
                    for line in lines:
                        detection, count = line.strip().split('\t')
                        count = int(count)

                        if detection in detection_count:
                            detection_count[detection] += count
                        else:
                            detection_count[detection] = count
                        total_count += count
        
        for detection, count in detection_count.items():
            percent = (count / total_count) * 100
            dest_file.write(f'{detection}\t{percent:.2f}%\n')

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python reduce.py <destination_file> [input_files]")
        sys.exit(1)

    input_prefix = sys.argv[1]
    destination_file = sys.argv[2]

    reduce_detections(input_prefix, destination_file)
