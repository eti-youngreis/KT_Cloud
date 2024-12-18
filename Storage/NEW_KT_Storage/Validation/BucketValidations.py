import re
import Storage.NEW_KT_Storage.Validation.GeneralValidations as GeneralValidations

def check_required_params(bucket_name, owner=None):
    if owner:
       return GeneralValidations.check_required_params(['bucket_name', 'owner'], {'bucket_name': bucket_name, 'owner': owner})
    else:
        return GeneralValidations.check_required_params(['bucket_name'], {'bucket_name': bucket_name})


def is_length_range(bucket_name):
    return GeneralValidations.is_length_in_range(bucket_name, 3, 63)


def is_valid_owner(owner):
    return GeneralValidations.is_valid_user_group_name(owner)


def bucket_exists(buckets, bucket_name):
    '''Check if a bucket with the given name already exists.'''
    return any(bucket.bucket_name == bucket_name for bucket in buckets)


def is_length_owner_valid(owner):
    return GeneralValidations.is_length_in_range(owner,3,125)


def is_bucket_name_valid(bucket_name):
    return re.match(r'^[a-zA-Z0-9\-]+$', bucket_name)


def is_region_valid(region):
    return re.match(r'^[a-z]{2}-[a-z]+-\d{1,2}$',region)


def valid_type_paramters(bucket_name):
    return type(bucket_name) == str
