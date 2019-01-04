"""
Upcat is a command line tool that allows users to edit and read catalog
files within the AEON environment.

Author: Anthony Chao (achao)
"""


import sys
import argparse


PRIMARY_KEY_FILES = ["Centers.catalog.test",
                     "Environments.catalog.test",
                     "Labs.catalog.test",
                     "Products.catalog.test"]

COMPOSITE_KEY_FILES = ["Services.catalog.test"]


def add(args):
    """Adds new key row to file."""

    if args.file in PRIMARY_KEY_FILES:
        with open(args.file, 'r') as infile:
            keys = get_keys(args.file)

            if args.key in keys:
                print("Provided key: '{}' already exists in {}, EXITING.".
                      format(args.key, args.file))
                exit(1)
            infile.seek(0)

            file_header_length = len(infile.readline().split('|'))
            data = [''] * file_header_length
            data[0] = args.key
            data = '|'.join(data) + '\n'
        with open(args.file, 'a') as infile:
            infile.write(data)
        print("Added '{}' to {}.".
              format(args.key, args.file))

    elif args.file in COMPOSITE_KEY_FILES:
        # TODO (achao): Handle composite key for Services.catalog
        pass

    else:
        print("Please provide a valid catalog filename.")
        print("Valid files: {}".
              format(PRIMARY_KEY_FILES + COMPOSITE_KEY_FILES))
        exit(1)


def read(args):
    """Prints header and values for given key."""

    if args.file in PRIMARY_KEY_FILES:
        infile = open(args.file, 'r')

        # Get list of keys from file and return cursor to start of file
        keys = get_keys(args.file)

    # TODO (achao): Figure out what to do with duplicates

    #    from collections import Counter
    #    print(Counter(keys))
    #    for key in list(Counter(keys)):
    #        print(key, end="")

        infile.seek(0)

        # If optional key argument is not provided, print all lines of file
        line = infile.readline()
        if args.key is None:
            while line:
                print(line.rstrip())
                line = infile.readline()

        # If optional key argument is provided, print out data product
        # information line by line
        elif args.key in keys:
            headers = line.rstrip().split('|')
            data_product_information_dict = dict.fromkeys(headers)

            while line:
                items = line.rstrip().split('|')
                if args.key == items[0]:
                    if len(items) == len(headers):
                        for i, header in enumerate(headers):
                            data_product_information_dict[header] = items[i]
                    else:
                        print("Number of headers and values do not match.")
                line = infile.readline()

            for key, value in data_product_information_dict.items():
                print(f"{key:25} | {value:25}")
        else:
            print("Please provide a valid key, EXITING.")
            if args.key is not None:
                print("...First 10 potential keys in {} with string '{}': {}".
                      format(args.file, args.key,
                             list(filter(lambda k: args.key in k, keys))[:10]))
            exit(1)

        infile.close()
    elif args.file in COMPOSITE_KEY_FILES:
        # TODO (achao): Handle composite key for Services.catalog
        pass
    else:
        print("Please provide a valid catalog filename.")
        print("Valid files: {}".
              format(PRIMARY_KEY_FILES + COMPOSITE_KEY_FILES))
        exit(1)


def update(args):
    """Updates column values for specified file and key."""

    if args.file in PRIMARY_KEY_FILES:

        infile = open(args.file, 'r')
        outfile = open(args.file + '.out', 'w')

        # A. Get list of keys from file, confirm user provided key is valid
        keys = get_keys(args.file)
        infile.seek(0)
        if args.key not in keys:
            print("Please provide a valid key, EXITING.")
            if args.key is not None:
                print("...First 10 potential keys in {} with string '{}': {}".
                      format(args.file, args.key,
                             list(filter(lambda k: args.key in k, keys))[:10]))
            exit(1)

        # B. Get list of headers from file, store value and index in dictionary
        header_list = infile.readline()
        outfile.write(header_list)
        header_dictionary = {}
        for counter, value in enumerate(header_list.rstrip().split('|')):
            header_dictionary[value.lower()] = counter

        # C. Extract cols and vals from args, validate input, zip cols and vals
        vals = args.cols_vals[1::2]
        cols = [col.lower() for col in args.cols_vals[::2]]

        if len(vals) != len(cols):  # Confirm same number of columns and values
            print("Number of columns and values do not match, EXITING.")
            exit(1)
        elif len(cols) != len(set(cols)):  # Confirm no duplicate columns
            print("Duplicate columns provided, EXITING.")
            exit(1)
        else:  # Confirm no invalid columns
            for col in cols:
                if col not in header_dictionary.keys():
                    print(f"'{col}' is not a valid column, EXITING.")
                    print("Valid columns are:",
                          list(header_dictionary.keys())[1:])
                    exit(1)

        cols_vals = zip(cols, vals)

        # D. Iterate through each line in file
        #       if provided key is found
        #           iterate through provided cols_vals and update
        line = infile.readline().lower()
        while line:
            items = line.split('|')
            if args.key == items[0]:
                for col_val in cols_vals:
                    print("Updating column: {0} | value: {1} => {2}.".format(
                        col_val[0].upper(),
                        items[header_dictionary[col_val[0]]].rstrip(),
                        col_val[1]))
                    items[header_dictionary[col_val[0]]] = col_val[1]
                if "\n" not in items[-1]:
                    items[-1] += "\n"
                outfile.write('|'.join(items))
            else:
                outfile.write(line)
            line = infile.readline()

        infile.close()
        outfile.close()

    elif args.file in COMPOSITE_KEY_FILES:
        # TODO (achao): Handle composite key for Services.catalog
        pass
    else:
        print("Please provide a valid catalog filename.")
        print("Valid files: {}".
              format(PRIMARY_KEY_FILES + COMPOSITE_KEY_FILES))
        exit(1)


def delete(args):
    """Deletes a row based on key provided."""

    if args.file in PRIMARY_KEY_FILES:
        with open(args.file, 'r') as infile:
            keys = get_keys(args.file)

            if args.key not in keys:
                print("Provided key: '{}' does not exist in {}, EXITING.".
                      format(args.key, args.file))
                exit(1)
            lines = infile.readlines()

        if input("Are you sure you want to delete '{}' from {}? (y/n) ".
                 format(args.key, args.file)) != "y":
            exit(1)

        with open(args.file, 'w') as infile:
            for line in lines:
                if line.split('|')[0] != args.key:
                    infile.write(line)
        print("Deleted '{}' from {}.".
              format(args.key, args.file))

    elif args.file in COMPOSITE_KEY_FILES:
        # TODO (achao): Handle composite key for Services.catalog
        pass

    else:
        print("Please provide a valid catalog filename.")
        print("Valid files: {}".
              format(PRIMARY_KEY_FILES + COMPOSITE_KEY_FILES))
        exit(1)


def get_keys(file):
    """Returns keys for given file."""
    if file in PRIMARY_KEY_FILES:
        with open(file, "r") as infile:
            keys = [line.split("|")[0] for line in infile.readlines()][1:]
    return keys


def main():
    """Main execution block."""

    parser = argparse.ArgumentParser(
        prog='upcat',
        description='summary: command line tool to edit catalog files')
    subparsers = parser.add_subparsers(help='<sub-command> [-h] [<args>]')

    # Sub-command: Update
    parser_update = subparsers.add_parser(
        "update",
        help="Updates column values for given file and key")
    parser_update.add_argument("file", help="filename to edit")
    parser_update.add_argument("key", help="primary key in provided file")
    parser_update.add_argument("cols_vals", nargs="+",
                               help="column value pairs to edit | \
                               usage: [col1 val1 col2 val2 ...]")
    parser_update.set_defaults(func=update)

    # Sub-command: Read
    parser_read = subparsers.add_parser(
        'read',
        help='Reads file line by line unless a -k/--key is specified')
    parser_read.add_argument("file", help="filename to read")
    parser_read.add_argument(
        "-k", "--key", default=None, metavar='FOO',
        help="key in file to reference")
    parser_read.set_defaults(func=read)

    # Sub-command: Add
    parser_read = subparsers.add_parser(
        'add',
        help='Adds given to key to file')
    parser_read.add_argument("file", help="filename to append key row to")
    parser_read.add_argument("key", help="key in file to reference")
    parser_read.set_defaults(func=add)

    # Sub-command: Delete
    parser_read = subparsers.add_parser(
        'delete',
        help='Deletes row in file for given key')
    parser_read.add_argument("file", help="filename to delete key row from")
    parser_read.add_argument("key", help="key in file to reference")
    parser_read.set_defaults(func=delete)

    args = parser.parse_args(None if sys.argv[1:] else ['-h'])
    return args.func(args)


if __name__ == "__main__":
    main()
