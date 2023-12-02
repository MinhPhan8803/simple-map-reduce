import sys
import re

def map_filter(input_file, output_prefix, regex):
    # Compile the regular expression
    pattern = re.compile(regex)

    # Open the input file
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # Filter lines based on the regular expression
    for line in lines:
        if pattern.search(line):
            # Write the matching line to the output file
            with open(f'{output_prefix}_filtered', 'a') as out_file:
                out_file.write(line)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python map.py <input_file> <output_prefix> <regex>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_prefix = sys.argv[2]
    regex = sys.argv[3]

    map_filter(input_file, output_prefix, regex)
