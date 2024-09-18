import re
from Storage.NEW_KT_Storage.Validation.GeneralValidations import *



def is_version_onject_name_valid(cluster_name):
    return is_length_in_range(cluster_name, 5, 25)

def validate_is_latest(is_latest):
    if not isinstance(is_latest, bool):
        raise ValueError("IsLatest must be a Boolean value (True or False).")


def validate_size(size):
    if not isinstance(size, int) or size < 0:
        raise ValueError("The size of the object must be a positive integer.")
