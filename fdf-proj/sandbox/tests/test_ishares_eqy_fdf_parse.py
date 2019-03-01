import pytest
import ishares_eqy_fdf_parse as s


def test_parse_data():
    data = ['A Lvl', 'A1', 'A2', '', 'B Lvl', 'B1']
    start = 'A Lvl'
    end = ''
    assert s.parse_data(data, start, end) == ['A1', 'A2']


def test_transpose():
    data = ['Fund Ticker,IOGP', 'Fund ISIN,IE00B6R51Z18']
    assert s.transpose(data) == ['Fund Ticker|Fund ISIN', 'IOGP|IE00B6R51Z18']


def test_format_date():
    date_l_1 = ['Jan 24 2019', 'Oct 7 1991']
    date_l_2 = ['Jan 24 2019|1', 'Oct 7 1991|1']
    assert s.format_date(date_l_1) == ['2019-01-24', '1991-10-07']
    assert s.format_date(date_l_2) == ['2019-01-24|1', '1991-10-07|1']


def test_format_header():
    assert s.format_header(['    a, b c, D', '1,2,3']) == ['a|b_c|d', '1,2,3']


def test_merge_holdings():
    data = [['A,B,C', '1,2,3', 'REMOVE ME,,'], ['B,C,D', '4,5,6']]
    data_merged = ['a|b|c|d', '1|2|3|', '|4|5|6']
    assert s.merge_holdings(data) == data_merged
