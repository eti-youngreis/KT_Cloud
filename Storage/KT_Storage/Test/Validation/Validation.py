import re
from typing import Optional,Dict

def is_valid_bucket_name(bucket_name: str) -> bool:
    '''Check if the bucket name is valid.'''
    # Example regex for a valid bucket name, adjust as needed
    # Assumes valid bucket names contain only letters, digits, and underscores
    pattern = r'^[\w-]+$'
    return bool(re.match(pattern, bucket_name))

def is_valid_policy_name(policy_name: str) -> bool:
    '''Check if the policy name is valid.'''
    # Example regex for a valid policy name, adjust as needed
    # Assumes valid policy names are between 1 and 255 characters
    # and contain only letters, digits, hyphens, and underscores
    pattern = r'^[\w-]{1,255}$'
    return bool(re.match(pattern, policy_name))

def validate_tags(tags: Optional[Dict]) -> bool:
    '''Check if the tags are valid. Tags should be a dictionary with string keys and values.'''
    if tags is None:
        return True
    if not isinstance(tags, dict):
        return False
    return all(isinstance(k, str) and isinstance(v, str) for k, v in tags.items())
