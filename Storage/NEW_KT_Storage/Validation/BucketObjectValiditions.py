import re

from Storage.NEW_KT_Storage.Validation.GeneralValidations import is_length_in_range, check_required_params


def is_bucket_object_name_valid(cluster_name):
    return is_length_in_range(cluster_name.strip(), 1, 20)

def check_required_params_object(kwargs):
    return check_required_params(['bucket_name', 'object_key'],kwargs)

def is_valid_object_name(engine_name: str) -> bool:
    '''Check if the engine name is valid.'''
    # Example regex for a valid engine name, adjust as needed
    # Assumes valid engine names contain only letters, digits, and underscores
    pattern = r'^[a-zA-Z0-9._-]{1,1024}$'
    return bool(re.match(pattern, engine_name))

