from NEW_KT_DB.Service.Classes.DBClusterParameterGroupService import DBClusterParameterGroupService
from typing import Optional, Dict

class DBClusterParameterGroupController:
    def __init__(self, service: DBClusterParameterGroupService):
        self.service = service

    def create_db_cluster_parameter_group(self, group_name: str, group_family: str, description: Optional[str]=None):
        return self.service.create(group_name, group_family, description)

    def delete_db_cluste_parameter_group(self, group_name: str):
        self.service.delete(group_name)

    def describe_db_cluste_parameter_group(self, group_name: str = None, max_records: int = 100, marker: str = None) -> Dict:
        return self.service.describe_group('DBClusterParameterGroup', group_name, max_records, marker)
    
    def modify_db_cluste_parameter_group(self, group_name: str, parameters: list[Dict[str, any]]):
        self.service.modify('DBClusterParameterGroup', group_name, parameters)