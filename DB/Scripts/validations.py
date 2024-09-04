import sqlite3
from sqlite3 import OperationalError
import re
import sys

def string_in_dict(string: str, values: dict) -> bool:
    '''Check if the string is in dict.'''
    return string in values

def is_length_in_range(string: str, min_length: int, max_length: int) -> bool:
    '''Check if the string is valid based on the length.'''
    return min_length <= len(string) <= max_length

def is_string_matches_regex(string: str, pattern: str) -> bool:
    '''Check if the optionGroupName is valid based on the pattern.'''
    return bool(re.match(pattern, string))

def is_json_column_contains_key_and_value(conn: sqlite3.Connection, table_name: str, column_name: str, key: str, value: str) -> bool:
    '''Check if a specific key-value pair exists within a JSON column in the given table.'''
    try:
        c = conn.cursor()
        c.execute(f'''SELECT COUNT(*) FROM {table_name}WHERE {column_name} LIKE ?''', (f'%{key}: {value}%',))
        return c.fetchone()[0] > 0
    except OperationalError as e:
        print(f'Error: {e}')

def is_valid_number(num: int, min: int = -sys.maxsize - 1, max: int = sys.maxsize) -> bool:
    return min <= num <= max