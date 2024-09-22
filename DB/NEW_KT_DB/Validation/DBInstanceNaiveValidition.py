import re

from Exceptions.DBInstanceNaiveException import ParamValidationError, MissingRequireParamError


def check_required_params(required_params, kwargs):
    """Check if all required parameters are present in kwargs."""
    for param in required_params:
        if param not in kwargs:
            raise MissingRequireParamError(f"Missing required parameter in input: {param}")


def check_extra_params(all_params, kwargs):
    string_all_params = ", ".join(all_params)
    for param in kwargs:
        if param not in all_params:
            raise ParamValidationError(f"Unknown parameter in input: {param}, must be one of: {string_all_params}")


def is_valid_db_instance_identifier(identifier, length):
    pattern = r'^[a-zA-Z][a-zA-Z0-9-]*[a-zA-Z0-9]$'
    return ((1 <= len(identifier) <= length) and re.match(pattern, identifier) and '--' not in identifier)