import sys

def reduce_words(input_files, destination_file):
    # Read the input file
    total_count = 0

    for input_file in input_files:
        with open('/home/sdfs/mrin/' + input_file, 'r') as file:
            lines = file.readlines()

        # Assume the format is 'word\tcount\n'
        word, count = lines[0].strip().split('\t')
        total_count += int(count)

    # Write the result to the destination file
    with open('/home/sdfs/mrout/' + destination_file, 'a') as file:
        print("??????????")
        file.write(f'{total_count}\n')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python reduce.py <input_file> <destination_file>")
        sys.exit(1)

    destination_file = sys.argv[1]
    input_files = sys.argv[2:]

    reduce_words(input_files, destination_file)
