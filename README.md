# CS 425 MP 4
## Simple Map Reduce by Minh Phan (minhnp2) and Shivam Pankaj Kumar (shivamk4)
## Getting Started
This project is built on Rust. You need the following steps to run:
1. To install Rust, use the following command on your system and then follow instructions:  
```bash 
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2. Then clone the project: 
```bash 
git clone https://gitlab.engr.illinois.edu/shivamk4/cs-425-mp-4.git  
```

3. Now on every machine get the nodes up and running:
```bash
    cd cs-425-mp-4/sdfs
    cargo build --release
    cargo run --release
```

An input field will spin up, and you can input your commands/requests.

Note: The scripts used for map and reduce operations must be Python scripts.

## List of available commands:
1. Listing the nodes's membership list (stored using ip addresses):
```bash
    list_mem
```

2. Listing the nodes's own ip:
```bash
    list_self
```

3. Leaving the system:
```bash
    leave
```

4. PUT'ing file onto the filesystem:
```bash
    put <local_file_path> <remote_file_name>
```
Example:
```bash
    put /home/tmp/local_file.dat remote_file.dat
```

5. GET'ing file from the filesystem:
```bash
    get <remote_file_name> <local_file_path>
```
Example:
```bash
    get remote_file.dat /home/tmp/local_file.dat
```

6. Listing nodes storing a particular file:
```bash
    ls <remote_file_name>
```
Example:
```bash
    ls remote_file.dat
```

7. Listing files stored by this particular node:
```bash
    store
```

8. Initiate GET from the same file on the SDFS by multiple nodes (multi-read):
```bash
    multiread <remote_file_name> <local_file_path> <ip_1> <ip_2> <ip_3> ..
```
The ip's are your nodes' ip addresses. You can add however many ip's as you like.
Example:
```bash
    multiread remote_file.dat /home/tmp/local_file.dat 127.0.0.1 128.0.0.1 129.0.0.1 130.0.0.1
```

9. Perform a map operation:
```bash
    maple <local_python_script_path> <num_tasks> <output_prefix> <remote_source_directory> <executable argument 1> <executable argument 2> ..
```
You can add how many executable arguments as you want.
The following example puts a dataset onto the file system then performs a regex search:
```bash
    put dataset.csv dataset.csv
    maple /home/scripts/regex_search_map.py 7 regex dataset \w*
```

10. Perform a reduce operation:
```bash
    juice <local_python_script_path> <num_tasks> <input_prefix> <output_file_name> <true|false>
```
For the last argument, input `true` or `false` to denote whether to delete the input files.
The following example is a follow up from the previous one:
```bash
    juice /home/scripts/regex_search_reduce.py 7 regex search_output.txt true
```

11. Performs a sequel filter using regex:
```bash
    SELECT ALL FROM <dataset_directory> WHERE <regex>
```
The examples from map and reduce can be shortened as:
```bash
    SELECT ALL FROM dataset WHERE \w*
```
Note how you don't need to provide an executable, and don't need to wrap the regex string in quotes.
The output file name will be `dataset_filter`

12. Performs a sequel join using regex:
```bash
    SELECT ALL FROM <dataset_1_directory> <dataset_2_directory> WHERE <d1_field> = <d2_field>
```
There must be spaces around `=`.
The following example uploads 2 datasets to the filesystem, then performs a join:
```bash
    put cars.csv cars.csv
    put trucks.csv trucks.csv
    SELECT ALL FROM cars trucks WHERE cars.price = trucks.price
```
