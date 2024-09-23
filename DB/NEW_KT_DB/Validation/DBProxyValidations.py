
from DB.NEW_KT_DB.Validation.GeneralValidations import *


def validate_db_proxy_name(db_proxy_name):
    """
    Validate the given database proxy name.

    The database proxy name must adhere to the following rules:
    - Start with a letter
    - Contain only ASCII letters, digits, and hyphens
    - Cannot end with a hyphen

    Parameters:
    db_proxy_name (str): The name of the database proxy to validate.

    Raises:
    ValueError: If the db_proxy_name is invalid.
    """
    if not is_valid_db_instance_identifier(db_proxy_name, 100):
        raise ValueError("Invalid DB Proxy name. Must start with a letter, contain only ASCII letters, digits, and hyphens, and cannot end with a hyphen")


def validate_init_db_proxy_params(attributes, required_params, all_params):
    """
    Validate the parameters provided against the required and allowed parameters.

    This function checks that:
    - All required parameters are present in the input attributes.
    - No extra parameters are included in the input attributes.

    Parameters:
    attributes (dict): The input parameters to validate.
    required_params (list): A list of parameters that must be present.
    all_params (list): A list of all allowed parameters.

    Raises:
    TypeError: If any required parameters are missing or if any invalid parameters are included.
    """
    if not check_required_params(required_params, attributes):
        raise TypeError("Missing required parameter in input")

    if not check_extra_params(all_params, attributes):
        raise TypeError("There is an invalid parameter in the input")
