import re
import Storage.NEW_KT_Storage.Validation.GeneralValidations as GeneralValidations

def check_required_params(key):
        return GeneralValidations.check_required_params(["key"],{'key': key})

def key_exists(tags, key):
    '''Check if a bucket with the given name already exists.'''
    return any(tag.key == key for tag in tags)

def is_valid_key_name(key):
    return re.match(r'^[a-zA-Z0-9._-]{2,63}$', key)

