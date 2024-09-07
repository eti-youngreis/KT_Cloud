from typing import Optional, Dict
from Service import DBClusterParameterGroupService

class DBClusterParameterGroupController:
    def __init__(self, service: DBClusterParameterGroupService):
        self.service = service

    def create_db_cluster_parameter_group(self, group_name: str, group_family: str, description: Optional[str]=None):
        self.service.create(group_name, group_family, description)

    def delete_db_cluste_parameter_group(self, group_name: str):
        self.service.delete(group_name)

    def describe_db_cluste_parameter_group(self, group_name: str) -> Dict:
        return self.service.describe(group_name)
    
    def modify_db_cluste_parameter_group(self, group_name: str, parameters: Optional[Dict[str, str]]=None):
        updates = {
            "parameters": parameters
        }
        self.service.modify(group_name, updates)
