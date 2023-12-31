import sys
import os

def reduce_filter(input_prefix, destination_file):
    # Aggregate filtered results from multiple map outputs
    with open('/home/sdfs/mrout/'+ destination_file, 'w') as dest_file:
        for filename in os.listdir('/home/sdfs/mrin'):
            if filename.startswith(input_prefix):
                try:
                    file = open('/home/sdfs/mrin/' + filename, 'r')
                except FileNotFoundError:
                    print("Error")
                else:
                    with file:
                        dest_file.write(file.read())
        dest_file.flush()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python reduce.py <input_prefix> <destination_file>")
        sys.exit(1)

    input_prefix = sys.argv[1]
    destination_file = sys.argv[2]

    reduce_filter(input_prefix, destination_file)
