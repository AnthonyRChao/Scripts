"""
Upcat is a command line tool that allows users to edit and read catalog
files within the AEON environment.

Author: Anthony Chao (achao)
"""


import sys
import argparse


def update(args):
    """Updates column values for specified file and key."""
    # TODO (achao): Handle compound key for Services.catalog

#     if args.file in ["Centers.catalog.test",
#                      "Environments.catalog.test",
#                      "Labs.catalog.test",
#                      "Products.catalog.test"]:
#         print("Valid file.")
#     elif args.file in ["Services.catalog.test"]:
#         print("TODO.")
#     else:
#         print("""Please provide a valid catalog filename.
# (e.g. Products.catalog)""")

    infile = open(args.file, 'r')
    outfile = open(args.file + '.out', 'w')

    # A. Get list of keys from file, confirm user provided key is valid
    keys = get_keys(infile)
    infile.seek(0)
    if args.key not in keys[1:]:
        print("Please provide a valid key.")

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
        print("Number of columns does not match number of values, exiting.")
        exit(1)
    elif len(cols) != len(set(cols)):  # Confirm no duplicate columns
        print("Duplicate columns provided, exiting.")
        exit(1)
    else:  # Confirm no invalid columns
        for col in cols:
            if col not in header_dictionary.keys():
                print(f"'{col}' is not a valid column, exiting.")
                print("Valid columns are:", list(header_dictionary.keys())[1:])
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
                print("Updating column: {0:15} | [{1}] => [{2}].".format(
                    col_val[0],
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


def read(args):
    """Prints header and values for given key."""

    # TODO (achao): Handle compound key for Services.catalog
    infile = open(args.file, 'r')

    # Get list of keys from file and return cursor to start of file
    keys = get_keys(infile)
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
        print("Please provide a valid key.")

    infile.close()


def get_keys(file):
    """Returns keys for given file."""
    return [line.split('|')[0] for line in file.readlines()]


def create():
    """Adds key and items to file."""


def delete():
    """Deletes a row based on key provided."""


def main():
    """Main execution block."""

    parser = argparse.ArgumentParser(
        prog='upcat',
        description='summary: command line tool to edit catalog files')
    subparsers = parser.add_subparsers(help='<sub-command> [-h] [<args>]')

    # Sub-command Update
    parser_update = subparsers.add_parser(
        'update',
        help='Updates given column-value pairs for key row in file')
    parser_update.add_argument("file", help="filename to edit")
    parser_update.add_argument("key", help="primary key in provided file")
    parser_update.add_argument("cols_vals", nargs="+",
                               help="column value pairs to edit | \
                               usage: [col1 val1 col2 val2 ...]")
    # parser_update.add_argument("columns", help="columns to edit")
    # parser_update.add_argument("values", help="values to update to")
    parser_update.set_defaults(func=update)

    # Sub-command Read
    parser_read = subparsers.add_parser(
        'read',
        help='Returns header and values for given key')
    parser_read.add_argument("file", help="filename to read")
    parser_read.add_argument(
        "-k", "--key", default=None, metavar='FOO',
        help="key in file to reference")
    parser_read.set_defaults(func=read)

    args = parser.parse_args(None if sys.argv[1:] else ['-h'])
    args.func(args)


if __name__ == "__main__":
    main()
