import sys
import re

def map_words(input_file, output_prefix):
    # Open the input file
    with open(input_file, 'r') as file:
        text = file.read()

    # Split text into words using regular expression
    words = re.findall(r'\w+', text.lower())

    # Create a dictionary to count occurrences of each word
    word_count = {}
    for word in words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

    # Create output files for each word
    for word, count in word_count.items():
        with open(f'{output_prefix}_{word}', 'w') as file:
            file.write(f'{word}\t{count}\n')

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python map.py <input_file> <output_prefix>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_prefix = sys.argv[2]

    map_words(input_file, output_prefix)
