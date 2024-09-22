import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Exceptions.GeneralExeptions import ObjectNotFoundException

class DBClusterNotFoundException(ObjectNotFoundException):
    def __init__(self, object_name: str):
        super().__init__(f'DBCluster {object_name}')

class DBClusterAlreadyExists(Exception):
    def __init__(self, object_name: str):
        super().__init__(f'DBCluster {object_name} already exists')
class InvalidDBClusterArgument(Exception):
    def __init__(self, object_name: str):
        super().__init__(f'Argument {object_name} is invalid')
               
class MissingRequiredArgument(Exception):
    def __init__(self, object_name: str = ""):
        super().__init__(f'Missing required args for DBCluster {object_name}') 