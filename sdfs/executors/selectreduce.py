import sys
import os

def reduce_filter(input_prefix, destination_file):
    # Aggregate filtered results from multiple map outputs
    with open(destination_file, 'w') as dest_file:
        for filename in os.listdir('.'):
            if filename.startswith(input_prefix) and filename.endswith('_filtered'):
                with open(filename, 'r') as file:
                    dest_file.write(file.read())

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python reduce.py <input_prefix> <destination_file>")
        sys.exit(1)

    input_prefix = sys.argv[1]
    destination_file = sys.argv[2]

    reduce_filter(input_prefix, destination_file)
