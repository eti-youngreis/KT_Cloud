import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Exceptions.GeneralExeptions import ObjectNotFoundException

class DBSnapshotNotFoundException(ObjectNotFoundException):
    def __init__(self, object_name: str):
        super().__init__(f'DBSnapshot {object_name}')

class DBSnapshotAlreadyExists(Exception):
    def __init__(self, object_name: str):
        super().__init__(f'DBSnapshot {object_name} already exists')
class InvalidDBSnapshotArgument(Exception):
    def __init__(self, object_name: str):
        super().__init__(f'Argument {object_name} is invalid')
               
class MissingRequiredArgument(Exception):
    def __init__(self, object_name: str = ""):
        super().__init__(f'Missing required args for DBSnapshot {object_name}') 