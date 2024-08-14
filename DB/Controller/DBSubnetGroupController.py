from typing import List
from DB.Service.Classes.DBSubnetGroupService import DBSubnetGroupService


class DBSubnetGroupController:
    
    def __init__(self, service: DBSubnetGroupService):
        self.service = service
        
        
    def create_db_subnet_group(self, db_subnet_group_description:str, db_subnet_group_name: str, 
                               subnet_ids:List[str], tags:List[Tag] = None):
        return self.service.create(db_subnet_group_description, db_subnet_group_name, subnet_ids, tags)
    
    def delete_db_subnet_group(self, db_subnet_group_name: str):
        return self.service.delete(db_subnet_group_name)
    
    def describe_db_subnet_groups(self, db_subnet_group_name: str = None, filters: List[Filter]= None, 
                                  marker:str = None, max_records: int = 100):
        return self.service.describe(db_subnet_group_name, filters, marker, max_records)
    
    def modify_db_subnet_group(self, db_subnet_group_name: str, subnet_ids: List[str], 
                               db_subnet_group_description: str = None):
        return self.service.modify(db_subnet_group_name, subnet_ids, db_subnet_group_description)