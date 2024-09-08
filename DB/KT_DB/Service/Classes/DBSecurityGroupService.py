from typing import List
from DB.Service.Abc import DBO
from DataAccess import DataAccessLayer

# naive implementations for these functions can be found in Scripts/DBSecurityGroup.py
class DBSecurityService(DBO):
    def __init__(self, dal:DataAccessLayer):
        self.dal = dal
        
    def create(self, db_security_group_description: str, db_security_group_name: str, tags: List[Tag] = None):
        pass
    
    def delete(self, db_security_group_name: str):
        pass
    
    def describe(self, db_security_group_name: str = None, filters:List[Filter] = None, marker:str = None, 
                 max_records:int = 100):
        pass
    
    # aws doesn't have an implementation for this function
    def modify(self):
        pass
    
    
    