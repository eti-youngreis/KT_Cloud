import re
from typing import Optional,Dict
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

def is_valid_engine_name(engine_name: str) -> bool:
    '''Check if the engine name is valid.'''
    # Example regex for a valid engine name, adjust as needed
    # Assumes valid engine names contain only letters, digits, and underscores
    pattern = r'^[\w-]+$'
    return bool(re.match(pattern, engine_name))

def is_valid_number(num: int, min: int = -sys.maxsize - 1, max: int = sys.maxsize) -> bool:
    return min <= num <= max

def is_valid_optionGroupName(option_group_name: str) -> bool:
    '''Check if the option group name is valid.'''
    # Example regex for a valid option group name, adjust as needed
    # Assumes valid option group names are between 1 and 255 characters
    # and contain only letters, digits, hyphens, and underscores
    pattern = r'^[\w-]{1,255}$'
    return bool(re.match(pattern, option_group_name))

def validate_tags(tags: Optional[Dict]) -> bool:
    '''Check if the tags are valid. Tags should be a dictionary with string keys and values.'''
    if tags is None:
        return True
    if not isinstance(tags, dict):
        return False
    return all(isinstance(k, str) and isinstance(v, str) for k, v in tags.items())

def check_required_params(required_params, kwargs):

    '''Check if all required parameters are present in kwargs.'''
    return all(param in kwargs for param in required_params)

def check_extra_params(all_params, kwargs):
    '''Check if all parameters in kwargs are allowed.'''
    return all(param in all_params for param in kwargs)

def check_filters_validation(filters):
    '''Validate filters. Returns True if valid, False otherwise.'''
    if not isinstance(filters, list):
        return False
    
    for filter_item in filters:
        if not (isinstance(filter_item, dict) and
                'Name' in filter_item and isinstance(filter_item['Name'], str) and
                'Values' in filter_item and isinstance(filter_item['Values'], list) and
                all(isinstance(value, str) for value in filter_item['Values'])):
            return False
    
    return True

def is_valid_db_instance_identifier(identifier, length):
    pattern = r'^[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]$'
    return ((1 <= len(identifier) <= length) and re.match(pattern, identifier) and '--' not in identifier)

def is_valid_user_group_name(input_string):
    if len(input_string) < 1 or len(input_string) > 128:
        return False
    pattern = r'^[a-zA-Z0-9_+=,.@-]+$'
    return re.match(pattern, input_string)
