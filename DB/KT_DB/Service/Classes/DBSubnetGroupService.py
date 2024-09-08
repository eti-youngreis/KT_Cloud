from typing import List
from DB.DataAccess import DataAccessLayer
from DB.Service.Abc import DBO
from DB.Service.Classes import Filter

# naive implementations for these functions can be found in Scripts/DBSubnetGroup.py
class DBSubnetGroupService(DBO):
    def __init__(self, dal:DataAccessLayer):
        self.dal = dal
        
    def create(self, db_subnet_group_description:str, db_subnet_group_name: str, subnet_ids:List[str], 
               tags:List[Tag] = None):
        pass
    
    def delete(self, db_subnet_group_name: str):
        pass
    
    def describe(self, db_subnet_group_name: str = None, filters: List[Filter]= None, marker:str = None,
                 max_records: int = 100):
        pass
    
    def modify(self, db_subnet_group_name: str, subnet_ids: List[str], db_subnet_group_description: str = None):
        pass