import sys
import os

def reduce_words(input_prefix, destination_file):
    with open('/home/sdfs/mrout/'+ destination_file, 'w') as dest_file:
        total_count = 0
        for filename in os.listdir('/home/sdfs/mrout'):
            if filename.startswith(input_prefix):
                with open(filename, 'r') as file:
                    lines = file.readlines()
                    # Assume the format is 'word\tcount\n'
                    _word, count = lines[0].strip().split('\t')
                    total_count += int(count)
        dest_file.write(f'{total_count}\n')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python reduce.py <input_file> <destination_file>")
        sys.exit(1)

    input_prefix = sys.argv[1]
    destination_file = sys.argv[2]

    reduce_words(input_prefix, destination_file)
