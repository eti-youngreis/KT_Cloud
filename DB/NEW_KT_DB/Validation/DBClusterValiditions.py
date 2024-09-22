import re
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Validation import GeneralValidations
import Exceptions.DBClusterExceptions as DBClusterExceptions

def validate_db_cluster_identifier(identifier: str) -> bool:
    """
    Validates the DBClusterIdentifier based on the cluster type.
    Returns True if valid, False otherwise.
    """
    length_constraint = 52
    pattern = r'^[a-zA-Z][a-zA-Z0-9\-]{0,' + str(length_constraint - 1) + r'}(?<!-)(?!.*--).*$'
    
    if re.match(pattern, identifier) is None:
        raise DBClusterExceptions.InvalidDBClusterArgument(identifier)

def validate_engine(engine: str) -> bool:
    """
    Validates the engine type.
    Reuses `string_in_dict` for engine validation.
    """
    valid_engines = ['mysql', 'postgres']
    if not GeneralValidations.string_in_dict(engine, dict.fromkeys(valid_engines, True)):
        raise DBClusterExceptions.InvalidDBClusterArgument(engine)

def validate_database_name(database_name: str) -> bool:
    """
    Validates the database name (if provided).
    Returns True if valid, False otherwise.
    """
    if database_name and not  GeneralValidations.is_length_in_range(database_name, 1, 64) or not GeneralValidations.is_string_matches_regex(database_name, r'^[a-zA-Z0-9]+$'):
        raise DBClusterExceptions.InvalidDBClusterArgument(database_name)

def validate_db_cluster_parameter_group_name(group_name: str) -> bool:
    """
    Validates the DBClusterParameterGroupName.
    Returns True if valid, False otherwise.
    """
    if group_name and re.match(r'^[a-zA-Z0-9\-]+$', group_name) is None:
        raise DBClusterExceptions.InvalidDBClusterArgument(group_name)

def validate_db_subnet_group_name(subnet_group_name: str) -> bool:
    """
    Validates the DBSubnetGroupName.
    Returns True if valid, False otherwise.
    """
    if subnet_group_name and  re.match(r'^[a-zA-Z0-9\-]+$', subnet_group_name) is None:
        raise DBClusterExceptions.InvalidDBClusterArgument(subnet_group_name)

def validate_port(port: int) -> bool:
    """
    Validates the port number.
    Returns True if valid, False otherwise.
    """
    if not GeneralValidations.is_valid_number(port, 1150, 65535):
        raise DBClusterExceptions.InvalidDBClusterArgument(port)

def validate_master_username(username: str) -> bool:
    """
    Validates the MasterUsername.
    Constraints:
    - Must be 1 to 16 letters or numbers.
    - First character must be a letter.
    - Can't be a reserved word for the chosen database engine.
    """
    if username and not GeneralValidations.is_length_in_range(username, 1, 16):
        raise DBClusterExceptions.InvalidDBClusterArgument(username)
    if username and not username[0].isalpha():  # First character must be a letter
        raise DBClusterExceptions.InvalidDBClusterArgument(username)
    if not GeneralValidations.is_string_matches_regex(username, r'^[a-zA-Z0-9]+$') : # Letters and numbers only
        raise DBClusterExceptions.InvalidDBClusterArgument(username)
    
def validate_master_user_password(password: str, manage_master_user_password: bool) -> bool:
    """
    Validates the MasterUserPassword.
    Constraints:
    - Must contain from 8 to 41 characters.
    - Can contain any printable ASCII character except "/", "\"", or "@".
    - Can't be specified if ManageMasterUserPassword is turned on.
    """
    if password and manage_master_user_password:
        raise DBClusterExceptions.InvalidDBClusterArgument(password)
    if not GeneralValidations.is_length_in_range(password, 8, 41):
        raise DBClusterExceptions.InvalidDBClusterArgument(password)
    # Ensure password doesn't contain "/", "\"", or "@"
    restricted_chars = ["/", "\\", "@"]
    for char in restricted_chars:
        if char in password:
            raise DBClusterExceptions.InvalidDBClusterArgument(password)
    # Optionally, check for only printable ASCII characters (32 to 126 ASCII range)
    if not all(32 <= ord(c) <= 126 for c in password):
        raise DBClusterExceptions.InvalidDBClusterArgument(password)

def check_required_params(required_params, **kwargs):
    for param in required_params:
        if param not in kwargs.keys():
            raise DBClusterExceptions.MissingRequiredArgument(param)