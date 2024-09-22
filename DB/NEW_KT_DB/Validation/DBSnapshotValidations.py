import re
import sys
from GeneralValidations import is_length_in_range, is_valid_number, is_valid_db_instance_identifier
from typing import Optional,Dict

def is_valid_db_snapshot_description(description_snapshot: str) -> bool:
    return is_length_in_range(description_snapshot, 1, 40)


def is_valid_progress(progress: str) -> bool:
    num_of_progress = progress[:-1]
    num_of_progress_int = int(num_of_progress)
    return is_valid_number(num_of_progress_int, 0, 100)


def is_valid_date(date) -> bool:
    pattern = r'^\d{4}-\d{2}-\d{2}$'  # Date format: YYYY-MM-DD
    return bool(re.match(pattern, date))

# db_instance_identifier

def is_valid_db_instance_id(db_instance_identifier: str) -> bool:
    return is_valid_db_instance_identifier(db_instance_identifier, 15)


def is_valid_url_parameter(url_snapshot: str) -> bool:
    '''Check if the url_snapshot parameter is valid.'''
    pattern = r'^[\w\-]+(?:%[0-9A-Fa-f]{2})*$'
    return bool(re.match(pattern, url_snapshot))


