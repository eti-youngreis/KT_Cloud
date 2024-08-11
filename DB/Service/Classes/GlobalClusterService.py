from typing import List
from DB.DataAccess import DataAccessLayer
from DB.Service.Abc import DBO


class GlobalClusterService(DBO):

    def __init__(self, dal: DataAccessLayer):
        self.dal = dal

    def create(self, database_name:str=None, deletion_protection:bool=None, engine:str=None, engine_lifecycle_support:str=None, 
               engine_version:str=None, global_cluster_identifier:str=None, source_db_cluster_identifier:str=None, storage_encrypted:bool=None):
        pass
    
    def delete(self, global_cluster_identifier: str):
        pass
    
    def describe(self, filters: List[Filter] = None, global_cluster_identifier: str = None, marker:str = None, max_records:int = 100):
        pass
    
    def modify(self, allow_major_version_upgrade:bool = None, deletion_protection:bool = None, engine_version:str = None,
               global_cluster_identifier: str = None, new_global_cluster_identifier:str = None):
        pass

    def switchover(self, global_cluster_identifier:str, target_db_cluster_identifier:str):
        pass