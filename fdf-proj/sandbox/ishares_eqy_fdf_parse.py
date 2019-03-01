#!/usr/bin/env python3

"""
Parse semi-structured iShares daily EQY FDF files into well-structured
pipe delimited flatfiles suitable for ETL Pipeline/database ingestion.

Author: Anthony Chao
Created: 02-11-2019
Last Edited: 02-28-2019
"""


import sys
import os
import argparse
import logging

from subprocess import run
from pathlib import Path
from datetime import datetime
from itertools import zip_longest
from time import time

import pandas as pd


LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_DATEFMT = '%d-%b-%y %H:%M:%S'
START = time()
GRAINS = {
    'fund': 'Fund Level',
    'basket': 'Basket Level',
    'spreads': 'Spreads',
    'allocations': 'Allocation Details',
    'holdings': ['Holdings: Securities', 'Holdings: Synthetics'],
    'fx': 'FX Rates',
    'forwards': 'FX Forwards',
    'swaps': 'Swaps'
    }

ORIGINAL_DIRECTORY = os.getcwd()
HOME_DIRECTORY = str(Path.home())
TARGET_DIRECTORY = os.path.join(HOME_DIRECTORY,
                                'workspaces',
                                'fdf-proj',
                                'csv_files',
                                '20190124_files')


def parse_args():
    """Parses user provided arguments with argparse's ArgumentParser."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--inputfile', required=True,
                        help='Input file to process')
    parser.add_argument('-o', '--outputfile', required=True,
                        help='Output file to write to')
    parser.add_argument('-g', '--grain', required=True,
                        help='Dictates the grain of data we are seeking')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='increase output verbosity')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='More information than you want')
    parser.add_argument('-q', '--quiet', help='supress most feedback')
    parser.add_argument('-f', '--force',
                        help='NOT IMPLEMENTED. Replace file if it exists')
    parser.add_argument('-nh', '--noheaders',
                        help='NOT IMPLEMENTED. Do not ouput header columns')
    parser.add_argument('-nc', '--nocleanup',
                        help='NOT IMPLEMENTED. Leave a trail for inspection')
    args = parser.parse_args()
    return args


def parse_data(string_list, start, end):
    """Extracts a list from a list based on `start` and `end` strings.

    (1) Iterates through a list of strings (`string_list`) and searches for
    `start`. (2) From `start`'s index, iterates to find the
    first occurence of `end`. (3) Returns a list of values
    between `start`'s index + 1 and `end`'s index.
    For this script, `end` will usually be represented as ''.

    Args:
        `string_list`: A list of strings
        `start`: A string to search for
        `end`: A string to search for

    Returns:
        `string_list_parsed`: A list of comma-delimited strings that fall
        between `start` and `end` indices

    Example:
        parse_data(['A Lvl', 'A1', 'A2', '', 'B Lvl', 'B1'], 'A Lvl', '')
        >>> ['A1', 'A2']
    """
    logging.info('begin parsing for search string, {}'.format(start))

    for index, string in enumerate(string_list):
        if string.strip() == start:
            start_index = index
            logging.info('starting index found at index {}'
                         .format(start_index))
            break
    try:
        start_index
    except NameError:
        bailout('search string {} not found'.format(start))

    for index in range(start_index, len(string_list)):
        if string_list[index].strip() == end:
            end_index = index
            logging.info('ending index found at index {}'
                         .format(end_index))
            break
    try:
        end_index
    except NameError:
        bailout('ending string {} not found'.format(end))

    string_list_parsed = string_list[start_index + 1:end_index]
    return string_list_parsed


def transpose(string_list):
    """Transposes a list of comma delimited strings.

    Function used to parse headers for 'Fund Level', 'Basket Level',
    and 'Swaps' sections of the FDF file.

    Args:
        `string_list`: A list of comma delimited strings

    Returns:
        `string_list_transposed`: A list of pipe delimited strings

    Example:
        transpose(['Fund Ticker,IOGP', 'Fund ISIN,IE00B6R51Z18'])
        >>> ['Fund Ticker|Fund ISIN', 'IOGP|IE00B6R51Z18']
    """
    s_split = [row.split(',') for row in string_list]

    # Transpose
    for i, row in enumerate(s_split):
        if len(row) > 2:
            col_name, col_value = row[0] + '_date', row[2]
            row.pop()
            s_split.insert(i + 1, [col_name, col_value])
    s_txnpsd = list(map(list, zip_longest(*s_split)))

    # 'Settlement Date' field in Basket Level does not have a trailing comma.
    # Joining in the below list comprehension results in TypeError (None
    # instead of str). To handle this, map all values to `str`
    s_txnpsd_S = [list(map(str, row)) for row in s_txnpsd]

    # Replace all 'None' values with ''. Easier to work with downstream.
    s_txnpsd_S_R = [[i.replace('None', '') for i in r] for r in s_txnpsd_S]
    string_list_transposed = ['|'.join(row) for row in s_txnpsd_S_R]

    return string_list_transposed


def merge_holdings(data):
    """Merges a list of two lists (Securities and Synthetics data).

    Args:
        `data`: A list of lists of comma-delimited strings

    Returns:
        `result_writeable`: A list of pipe-delimited strings

    Example:
        merge_holdings([['A,B,C', '1,2,3', 'REMOVE ME,,'], ['B,C,D', '4,5,6']])
        >>> ['a|b|c|d', '1|2|3|', '|4|5|6']
    """
    # Input: Split (sec)urities and (syn)thetics data
    sec_data = [row.split(',') for row in data[0]]
    syn_data = [row.split(',') for row in data[1]]

    # Headers: Strip whitespace, lower, and replace spaces with underscores
    sec_h_temp = list(map(str.lower, [val.strip() for val in sec_data[0]]))
    syn_h_temp = list(map(str.lower, [val.strip() for val in syn_data[0]]))
    sec_h = [val.replace(' ', '_') for val in sec_h_temp]
    syn_h = [val.replace(' ', '_') for val in syn_h_temp]

    # Data Prep: Separate body (b) from header (h) to prep for dataframe
    # creation
    # (!!!) Last row of Holdings: Securities section contains aggregate
    # information for Deliverable Basket Qty and Pricing Basket Qty,
    # this is removed to prevent duplication
    sec_b = sec_data[1:len(sec_data) - 1]
    syn_b = syn_data[1:]

    # Dataframes: Create sec_df and syn_df
    sec_df = pd.DataFrame(sec_b, columns=sec_h)
    syn_df = pd.DataFrame(syn_b, columns=syn_h)

    # Dataframes: Merge, replace NaN's with '', convert values to a list
    merged_holdings_df = pd.concat([sec_df, syn_df]).fillna('')
    result = merged_holdings_df.values.tolist()

    # Combine header and body
    result.insert(0, list(merged_holdings_df))

    # Output
    result_writeable = ['|'.join(row) for row in result]
    return result_writeable


def bailout(message):
    """Logs error message and exits."""
    logging.error(message + ' --- ERROR, EXITING --- ... total elapsed time {}'
                  .format(time() - START))
    sys.exit(1)


def format_date(string_list):
    """Iterates through a list of pipe-delimited strings and converts all
    MMM DD YYY pattern dates to the YYYY-MM-DD pattern.

    Args:
        `string_list`: A list of pipe delimited strings

    Returns:

        `date_formatted_string_list`: A list of pipe delimited strings

    Example:
        format_date(['Jan 24 2019|1', 'Oct 7 1991|1])
        >>> ['2019-01-24', '1991-10-07]
    """
    logging.info('converting date formats from MMM DD YYYY'
                 ' to YYYY-MM-DD')
    date_values = 0
    non_date_values = 0
    date_formatted_string_list = []
    for string in string_list:
        targets = string.split('|')
        for i, target in enumerate(targets):
            try:
                date = datetime.strptime(target, '%b %d %Y')
                targets[i] = date.strftime('%Y-%m-%d')
                date_values += 1
            except ValueError:
                non_date_values += 1
        date_formatted_string_list.append('|'.join(targets))

    logging.debug('format_date() processed {} date values and {} non'
                  ' date values'
                  .format(date_values, non_date_values))
    return date_formatted_string_list


def format_header(string_list):
    """Modifies first value of a list of comma delimited strings.

    Split by comma, strip whitespace, format date, lowercase all, replace
    spaces with underscores, , set all fields to lowercase, and re-join
    with pipes.

    Args:
        `string_list`: List of comma delimited strings

    Returns:
        `string_list_formatted`: Formatted list of pipe delimited strings

    Example:
        format_header(['    a, b c, D', '1,2,3'])
        >>> ['a|b_c|d', '1,2,3']
    """
    logging.info('cleaning up header')
    # Separate body of string_list
    # TODO (achao 2/25/19) If len(string_list) == 1, will this throw an error?
    b = string_list[1:]

    # Modify header
    h_split = string_list[0].split(',')
    h_modify = [i.strip().lower().replace(' ', '_') for i in h_split]
    h = '|'.join(h_modify)

    # Rejoin header and body and return
    string_list_formatted = [h] + b
    return string_list_formatted


def confirm_file_exists(args_inputfile):
    """Checks if provided inputfile exists."""
    file_path = os.path.join(TARGET_DIRECTORY,
                             os.path.split(args_inputfile)[-1])
    if os.path.isfile(file_path):
        logging.info('confirmed inputfile: {} exists'
                     .format(os.path.split(args_inputfile)[-1]))
    else:
        bailout('inputfile: {} does not exist'
                .format(os.path.split(args_inputfile)[-1]))


def confirm_grain_is_valid(grain):
    """Checks if provided grain exists in GRAINS dict."""
    if grain in GRAINS.keys():
        logging.info('confirmed valid runtime grain provided: {}'
                     .format(grain))
    else:
        bailout('unrecognized runtime grain provided: {}. Available grains: {}'
                .format(grain, list(GRAINS.keys())))


def confirm_valid_date(file_contents):
    """Confirms that dates in the Fund Level section of the input file are
    identical and returns a formatted date.
    """
    logging.info('working on validating dates in Fund Level section of FDF ...'
                 ' PROCESSING')
    write_data = parse_data(file_contents, 'Fund Level', '')
    date_set = set()
    for row in write_data:
        if row.count(',') == 2 and row.split(',')[2] != '':
            date_set.add(row.split(',')[2])
    if len(date_set) == 1:
        date = datetime.strptime(next(iter(date_set)),
                                 '%b %d %Y').strftime('%Y-%m-%d')
        logging.info('validated that dates in Fund Level section match ...'
                     ' COMPLETE')
        return date
    else:
        bailout('dates in Fund Level section do not match, exiting. ')


def confirm_no_duplicates(data_list, outfile):
    """Confirms that rows to append to an outfile do not
    include duplicates via set intersection.

    Args:
        `data_list`: A list of strings to be appended to outfile
        `outfile`: A string for the name of file to check

    Returns:
        `boolean`: True if no duplicates, bailout() if there
        are duplicates
    """
    # Remove existing source_category, source_name, f_position_date columns
    existing = []
    with open(outfile, 'r') as outfile:
        existing_raw = outfile.readlines()
    for string_raw in existing_raw:
        string = '|'.join(string_raw.split('|')[3:])
        existing.append(string)

    # Remove header and add newline char to data_list for comparison
    data_list = [string + '\n' for string in data_list][1:]

    # Instantiate sets and intersection (items that are in both sets)
    existing_set = set(existing)
    data_list_set = set(data_list)
    intersection = existing_set & data_list_set

    if not intersection:
        logging.info('no overlap with existing rows, proceeding')
        return True
    else:
        bailout('{} row(s) to append to {} overlap with existing rows,'
                ' culprit(s): {}'
                .format(len(intersection), outfile.name, intersection))


def confirm_valid_isin(args_inputfile):
    """Confirms that Fund Name in file maps to an ISIN"""
    logging.info('executing query/SQL verification ...')
    with open(args_inputfile) as inputfile:
        rows = inputfile.readlines()
    for row in rows:
        cur = row.split(',')
        if cur[0] == 'Fund Name':
            query_1 = (
                'select isin'
                ' from public.v_etp_mkt_ibp_classification'
                ' where markit_issue_name= \'{}\''
                ' and record_is_current = \'Y\''
                ' and markit_issue_name not in'
                ' (select markit_issue_name'
                ' from public.v_etp_mkt_ibp_classification'
                ' where record_is_current = \'Y\''
                ' and markit_family=\'BlackRock\''
                ' and markit_issue_name is not null'
                ' and listing_region <> \'US\''
                ' group by markit_issue_name'
                ' having count(*) > 1)').format(cur[1].rstrip())
            res_object_1 = run(['query', 'aeon', query_1, 'kettle', 'blk-w'])
            type(run(['query', 'aeon', query_1, 'kettle', 'blk-w']))
            run(['query', 'aeon', query_1, 'kettle', 'blk-w'])
            run(['query', 'aeon', query_1, 'kettle', 'blk-w'])
            logging.info('command executed: {}'
                         .format(' '.join(res_object_1.args)))
            logging.info('command stdout: {}'.format(res_object_1.stdout))
            print('res_object_1:', res_object_1)
            if res_object_1.returncode == 1:
                logging.info('ISIN found in'
                             ' public.v_etp_mkt_ibp_classification via'
                             ' Fund Name')
                return 0

        elif cur[0] == 'Fund Ticker':
            query_2 = (
                'select isin'
                ' from public.v_etp_mkt_ibp_classification'
                ' where dixie_ticker= \'{}\''
                ' and record_is_current = \'Y\''
                ' and markit_family=\'BlackRock\''
                ' and dixie_ticker is not null'
                ' and listing_region <> \'US\''
                ' and dixie_ticker not in '
                ' (select dixie_ticker'
                ' from public.v_etp_mkt_ibp_classification'
                ' where record_is_current = \'Y\''
                ' and markit_family=\'BlackRock\''
                ' and dixie_ticker is not null'
                ' and listing_region <> \'US\''
                ' group by dixie_ticker'
                ' having count(*) > 1)').format(cur[1].rstrip())
            res_object_2 = run(['query', 'aeon', query_2, 'kettle', 'blk-w'])
            logging.info('command executed: {}'
                         .format(' '.join(res_object_2.args)))
            logging.info('command stdout: {}'.format(res_object_2.stdout))
            print('res_object_2:', res_object_2)
            if res_object_2.returncode == 0:
                logging.info('ISIN found in'
                             ' public.v_etp_mkt_ibp_classification from'
                             ' Fund Ticker')
                return 0

    bailout('ISIN not found in public.v_etp_mkt_ibp_classification'
            ' via Fund Name or Ticker')


def main():
    """Handles the actual logic of the script."""
    args = parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG,
                            format=LOG_FORMAT, datefmt=LOG_DATEFMT)
    elif args.verbose:
        logging.basicConfig(level=logging.INFO,
                            format=LOG_FORMAT, datefmt=LOG_DATEFMT)
    else:
        logging.basicConfig(level=logging.WARNING,
                            format=LOG_FORMAT, datefmt=LOG_DATEFMT)

    confirm_file_exists(args.inputfile)
    confirm_grain_is_valid(args.grain)
    confirm_valid_isin(args.inputfile)

    # READ from inputfile
    with open(args.inputfile, mode='r', encoding='utf-8-sig') as infile:
        infile_rows = infile.read().split('\n')
        f_position_date = confirm_valid_date(infile_rows)
        logging.info('working on READ process for relevant grain ...'
                     ' PROCESSING')
        logging.info('opened {} for reading'
                     .format(os.path.split(infile.name)[-1]))

        # Holdings: Securities and Holdings: Synthetics
        if args.grain == 'holdings':
            holdings_parsed_rows = []
            for i in range(len(GRAINS[args.grain])):
                holdings_parsed_rows.append(parse_data(infile_rows,
                                            GRAINS[args.grain][i],
                                            ''))
            outfile_rows = merge_holdings(holdings_parsed_rows)

        # FX Rates
        elif args.grain == 'fx':
            fx_header = 'currency,spot_rate'
            fx_rows = parse_data(infile_rows, GRAINS[args.grain], '')[1:]
            fx_rows.insert(0, fx_header)
            outfile_rows = fx_rows

        # FX Forwards
        elif args.grain == 'forwards':
            outfile_rows = format_header(parse_data(infile_rows,
                                                    GRAINS[args.grain], ''))
        # Spreads and Allocation Details
        elif args.grain in ['spreads', 'allocations']:
            sp_al_rows = format_header(parse_data(infile_rows,
                                                  GRAINS[args.grain], ''))
            outfile_rows = [string.replace(',', '|') for string in sp_al_rows]

        # Fund Level, Basket Level, and Swaps
        else:
            outfile_rows = format_header(
                format_date(
                    transpose(
                        parse_data(infile_rows, GRAINS[args.grain], ''))))

        logging.info('{} lines prepped to write to {}'
                     .format(len(outfile_rows), args.outputfile))
        logging.debug('outfile_rows to write: {}'
                      .format(outfile_rows))
        logging.info('completed READ process for relevant grain ...'
                     ' COMPLETE')

    # Check if writing header is required
    with open(args.outputfile, mode='r') as checkfile:
        informational_headers = 'source_category|source_name|f_position_date|'
        header = informational_headers + outfile_rows[0] + '\n'
        ignore_headers = False

        if header == checkfile.readline():
            ignore_headers = True

        logging.info('ignore_headers set to {}'.format(ignore_headers))
    if ignore_headers:
        # APPEND to outputfile
        logging.info('working on APPEND process for relevant grain ...'
                     ' PROCESSING')
        confirm_no_duplicates(outfile_rows, args.outputfile)
        with open(args.outputfile, mode='a') as outfile:
            for i in range(1, len(outfile_rows[1:]) + 1):
                outfile.write('iShares FTP|{}|{}|{}\n'
                              .format(os.path.split(args.inputfile)[-1],
                                      f_position_date,
                                      outfile_rows[i]))
        logging.info('appending data to {}'.format(args.outputfile))
        logging.debug('appending body as: {}'.format(outfile_rows[1:]))
        logging.info('completed APPEND process for relevant grain ...'
                     ' COMPLETE')
    else:
        # WRITE to outputfile
        logging.info('working on WRITE process for relevant grain ...'
                     ' PROCESSING')
        with open(args.outputfile, mode='w') as outfile:
            outfile.write(header)
            logging.debug('writing header as: {}'.format(header))
            for i in range(1, len(outfile_rows[1:]) + 1):
                outfile.write('iShares FTP|{}|{}|{}\n'
                              .format(os.path.split(args.inputfile)[-1],
                                      f_position_date,
                                      outfile_rows[i]))
        logging.info('wrote {} lines to {}'
                     .format(len(outfile_rows), args.outputfile))
        logging.debug('wrote {} lines to {}: {}'
                      .format(len(outfile_rows),
                              args.outputfile,
                              outfile_rows[1:]))
        logging.info('completed WRITE process for relevant grain ...'
                     ' COMPLETE')
        logging.info('--- SUCCESS --- total elapsed time: {} seconds'
                     .format(time() - START))


if __name__ == '__main__':
    main()
