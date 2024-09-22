from .GeneralExeptions import ObjectNotFoundException


class DBSubnetGroupNotFound(ObjectNotFoundException):
    def __init__(self, object_name: str):
        super().__init__(f'DBSubnetGroup {object_name}')


class DBSubnetGroupAlreadyExists(Exception):
    def __init__(self, object_name: str):
        super().__init__(f'DBSubnetGroup {object_name} already exists')

class InvalidDBSubnetGroupName(Exception):
    def __init__(self, object_name: str):
        super().__init__(f'DBSubnetGroup {object_name} is invalid')
        
class InvalidDBSubnetGroupDescription(Exception):
    def __init__(self, object_name: str):
        super().__init__(f'DBSubnetGroup {object_name} is invalid')
        
class MissingRequiredArgument(Exception):
    def __init__(self, object_name: str = ""):
        super().__init__(f'Missing required args for DBSubnetGroup {object_name}')