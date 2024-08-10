import re
from typing import Optional,Dict

def is_valid_engineName(engine_name: str) -> bool:
    """Check if the engine name is valid."""
    # Example regex for a valid engine name, adjust as needed
    # Assumes valid engine names contain only letters, digits, and underscores
    pattern = r'^[\w-]+$'
    return bool(re.match(pattern, engine_name))

def is_valid_optionGroupName(option_group_name: str) -> bool:
    """Check if the option group name is valid."""
    # Example regex for a valid option group name, adjust as needed
    # Assumes valid option group names are between 1 and 255 characters
    # and contain only letters, digits, hyphens, and underscores
    pattern = r'^[\w-]{1,255}$'
    return bool(re.match(pattern, option_group_name))

def validate_tags(tags: Optional[Dict]) -> bool:
    """Check if the tags are valid. Tags should be a dictionary with string keys and values."""
    if tags is None:
        return True
    if not isinstance(tags, dict):
        return False
    return all(isinstance(k, str) and isinstance(v, str) for k, v in tags.items())
