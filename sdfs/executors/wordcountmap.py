import sys
import re

def map_words(input_file, output_prefix):
    # Create a dictionary to count occurrences of each word
    word_count = {}

    # Open the input files
    with open('/home/sdfs/mrin/' + input_file, mode='r', errors='replace') as file:
        text = file.read()

    # Split text into words using regular expression
    words = re.findall(r'\w+', text.lower())

    for word in words:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

    # Create output files for each word
    for word, count in word_count.items():
        with open(f'/home/sdfs/mrout/{output_prefix}_{word}', 'w') as file:
            file.write(f'{word}\t{count}\n')

    for word in word_count.keys():
        print(word)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python map.py <output_prefix> [input_files]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_prefix = sys.argv[2]

    map_words(input_file, output_prefix)
