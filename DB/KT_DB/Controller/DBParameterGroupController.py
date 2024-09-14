from typing import Optional, Dict
from Service import DBParameterGroupService

class DBParameterGroupController:
    def __init__(self, service: DBParameterGroupService):
        self.service = service

    def create_db_parameter_group(self, group_name: str, group_family: str, description: Optional[str]=None):
        self.service.create(group_name, group_family, description)

    def delete_db_parameter_group(self, group_name: str):
        self.service.delete(group_name)

    def describe_db_parameter_group(self, group_name: str) -> Dict:
        return self.service.describe_group(group_name)
    
    def modify_db_parameter_group(self, group_name: str, parameters: list[Dict[str, any]]):
        # updates = {
        #     "parameters": parameters
        # }
        self.service.modify(group_name, parameters)
        
    def describe_db_parameters(self, db_parameter_group_name: str, source: str='user', max_records: int=100, marker: str=None):
        return self.service.describe_parameters(db_parameter_group_name, source, max_records, marker)
