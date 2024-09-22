import Exceptions.DBSubnetGroupExceptions as DBSubnetGroupExceptions
from Validation.GeneralValidations import *

def validate_subnet_group_name(name: str):
    if not name:
        raise DBSubnetGroupExceptions.InvalidDBSubnetGroupName(name)
    
    if not is_length_in_range(name, 1, 255):
        raise DBSubnetGroupExceptions.InvalidDBSubnetGroupName(
            name
        )

    

def validate_subnet_group_description(description: str):
    if description and not is_length_in_range(description, 1, 255):
        raise ValueError(
            "invalid length for subnet group description: "
            + len(description)
        )

