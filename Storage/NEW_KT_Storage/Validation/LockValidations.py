import re
import Storage.NEW_KT_Storage.Validation.GeneralValidations as GeneralValidations

def validate_lock_name(lock_name: str):
    '''Validate the lock name based on length and pattern.'''
    min_length = 3
    max_length = 64
    pattern = r'^(?!.*^\.)[a-zA-Z0-9-_]*\.[a-zA-Z0-9-_]+$'
    
    if not GeneralValidations.is_length_in_range(lock_name, min_length, max_length):
        raise ValueError(f"Lock name must be between {min_length} and {max_length} characters.")
    
    if not GeneralValidations.is_string_matches_regex(lock_name, pattern):
        raise ValueError("Lock name contains invalid characters.")
    
def validate_lock_exists(lock_id: str, locks_dict: dict):
    '''Check if the lock already exists in the given dictionary.'''
    if GeneralValidations.string_not_in_dict(lock_id, locks_dict):
        raise ValueError(f"The lock with ID '{lock_id}' does not exists.")
    
def validate_lock_does_not_exists(lock_id: str, locks_dict: dict):
    '''Check if the lock already exists in the given dictionary.'''
    if GeneralValidations.string_in_dict(lock_id, locks_dict):
        raise ValueError(f"The lock with ID '{lock_id}' already exists.")


def validate_lock_mode(lock_mode: str):
            if lock_mode not in ["read", "write", "all"]:
                raise ValueError(f"Invalid lock mode: {lock_mode}")
        
def validate_time_amount(amount: int, unit: str):
    '''Validate the amount of time based on the unit.'''
    if unit == "m":  
        if not GeneralValidations.is_valid_number(amount, 1, 100):
            raise ValueError("Amount for minutes must be between 1 and 100.")
    elif unit == "h":  
        if not GeneralValidations.is_valid_number(amount, 1, 100):
            raise ValueError("Amount for hours must be between 1 and 100.")
    elif unit == "d": 
        if not GeneralValidations.is_valid_number(amount, 1, 365):
            raise ValueError("Amount for hours must be between 1 and 365.")
    elif unit == "m":  
        if not GeneralValidations.is_valid_number(amount, 1, 24):
            raise ValueError("Amount for months must be between 1 and 24.")
    elif unit == "y":  
        if not GeneralValidations.is_valid_number(amount, 1, 3):
            raise ValueError("Amount for years must be between 1 and 3.")
    else:
        raise ValueError("Unsupported time unit.")


