

import re
def validate_target_role(TargetRole:str) -> bool:
    '''Check if target role is valid target role must be 'READ_WRITE'|'READ_ONLY'.'''
    return TargetRole in ['READ_WRITE','READ_ONLY']


def validate_name(name):
    pattern = r'^[a-zA-Z][a-zA-Z0-9]*(-[a-zA-Z0-9]+)*$'
    
    if re.match(pattern, name):
        return True
    else:
        return False

