from typing import List
from Service import GlobalClusterService


class GlobalClusterController:

    def __init__(self, service: GlobalClusterService):
        self.service = service

    def create_global_cluster(self, database_name: str = None, deletion_protection: bool = None,
                              engine: str = None, engine_lifecycle_support: str = None,
                              engine_version: str = None, global_cluster_identifier: str = None,
                              source_db_cluster_identifier: str = None, storage_encrypted: bool = None):
        return self.service.create(database_name, deletion_protection, engine, engine_lifecycle_support, engine_version,
                            global_cluster_identifier, source_db_cluster_identifier, storage_encrypted)
        
    def delete_global_cluster(self, global_cluster_identifier: str):
        return self.service.delete(global_cluster_identifier)
    
    
    def describe_global_clusters(self, filters: List[Filter] = None, global_cluster_identifier: str = None, marker:str = None, max_records:int = 100):
        return self.service.describe(filters, global_cluster_identifier, marker, max_records)
        
    def modify_global_cluster(self, allow_major_version_upgrade:bool = None, deletion_protection:bool = None, engine_version:str = None,
                              global_cluster_identifier: str = None, new_global_cluster_identifier:str = None):
        return self.service.modify(allow_major_version_upgrade, deletion_protection, engine_version, global_cluster_identifier, new_global_cluster_identifier)
        
    def switchover_global_cluster(self, global_cluster_identifier:str, target_db_cluster_identifier:str):
        return self.service.switchover(global_cluster_identifier, target_db_cluster_identifier)
