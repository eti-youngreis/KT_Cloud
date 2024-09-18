import re
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Validation import GeneralValidations

# def is_db_cluster_name_valid(cluster_name):
#     return is_length_in_range(cluster_name, 5, 20)

def validate_db_cluster_identifier(identifier: str) -> bool:
    """
    Validates the DBClusterIdentifier based on the cluster type.
    Returns True if valid, False otherwise.
    """
    length_constraint = 52
    pattern = r'^[a-zA-Z][a-zA-Z0-9\-]{0,' + str(length_constraint - 1) + r'}(?<!-)(?!.*--).*$'
    
    return re.match(pattern, identifier) is not None

def validate_engine(engine: str) -> bool:
    # """
    # Validates the engine type.
    # Returns True if valid, False otherwise.
    # """
    # valid_engines = ['aurora-mysql', 'aurora-postgresql', 'mysql', 'postgres', 'neptune']
    # return engine in valid_engines
    """
    Validates the engine type.
    Reuses `string_in_dict` for engine validation.
    """
    valid_engines = ['mysql', 'postgres']
    return GeneralValidations.string_in_dict(engine, dict.fromkeys(valid_engines, True))

def validate_database_name(database_name: str) -> bool:
    """
    Validates the database name (if provided).
    Returns True if valid, False otherwise.
    """
    # if not database_name:
    #     return True  # No validation needed if not provided
    # return re.match(r'^[a-zA-Z0-9]{1,64}$', database_name) is not None
    if not database_name:
        return True  # No validation needed if not provided
    return GeneralValidations.is_length_in_range(database_name, 1, 64) and GeneralValidations.is_string_matches_regex(database_name, r'^[a-zA-Z0-9]+$')

def validate_db_cluster_parameter_group_name(group_name: str) -> bool:
    """
    Validates the DBClusterParameterGroupName.
    Returns True if valid, False otherwise.
    """
    if not group_name:
        return True  # No validation needed if not provided
    return re.match(r'^[a-zA-Z0-9\-]+$', group_name) is not None

def validate_db_subnet_group_name(subnet_group_name: str) -> bool:
    """
    Validates the DBSubnetGroupName.
    Returns True if valid, False otherwise.
    """
    if not subnet_group_name:
        return True  # No validation needed if not provided
    return re.match(r'^[a-zA-Z0-9\-]+$', subnet_group_name) is not None

def validate_port(port: int) -> bool:
    """
    Validates the port number.
    Returns True if valid, False otherwise.
    """
    # return 1150 <= port <= 65535

    return GeneralValidations.is_valid_number(port, 1150, 65535)
# from general_validations import (
#     is_length_in_range,
#     is_string_matches_regex
# )

def validate_master_username(username: str) -> bool:
    """
    Validates the MasterUsername.
    Constraints:
    - Must be 1 to 16 letters or numbers.
    - First character must be a letter.
    - Can't be a reserved word for the chosen database engine.
    """
    if not username:
        return True  # Not required, so no validation needed if not provided
    if not GeneralValidations.is_length_in_range(username, 1, 16):
        return False
    if not username[0].isalpha():  # First character must be a letter
        return False
    # Optional: Add a list of reserved words for specific engines and check against that
    reserved_words = []  # Define reserved words for the engine if applicable
    if username.lower() in reserved_words:
        return False
    return GeneralValidations.is_string_matches_regex(username, r'^[a-zA-Z0-9]+$')  # Letters and numbers only

def validate_master_user_password(password: str, manage_master_user_password: bool) -> bool:
    """
    Validates the MasterUserPassword.
    Constraints:
    - Must contain from 8 to 41 characters.
    - Can contain any printable ASCII character except "/", "\"", or "@".
    - Can't be specified if ManageMasterUserPassword is turned on.
    """
    if not password:
        return True  # Not required, so no validation needed if not provided
    if manage_master_user_password:
        return False  # Can't specify password if ManageMasterUserPassword is turned on
    if not is_length_in_range(password, 8, 41):
        return False
    # Ensure password doesn't contain "/", "\"", or "@"
    restricted_chars = ["/", "\\", "@"]
    for char in restricted_chars:
        if char in password:
            return False
    # Optionally, check for only printable ASCII characters (32 to 126 ASCII range)
    if not all(32 <= ord(c) <= 126 for c in password):
        return False
    return True

def check_required_params(required_params, **kwargs):
    for param in required_params:
        if param not in kwargs.keys():
            return False
    return True
