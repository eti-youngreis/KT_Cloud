import re

from Storage.NEW_KT_Storage.Validation.GeneralValidations import check_required_params


def is_bucket_object_name_valid(cluster_name):
    pattern = r'^[a-zA-Z0-9._-]{1,1024}$'
    return bool(re.match(pattern, cluster_name))

def check_required_params_object(kwargs):
    return check_required_params(['bucket_name', 'object_key'],kwargs)

def is_bucket_name_valid(cluster_name):
    pattern = r'^[a-zA-Z0-9._-]{3,63}$'
    return bool(re.match(pattern, cluster_name))




