from typing import List

class FailoverState:
    
    """
    Represents the failover state of a database cluster.

    Attributes:
        from_db_cluster_arn (str): The ARN of the source database cluster.
        is_data_loss_allowed (bool): Indicates if data loss is allowed during failover.
        status (str): The status of the failover process.
        to_db_cluster_arn (str): The ARN of the target database cluster.
    """
    
    def __init__(self, from_db_cluster_arn: str = None,
                 is_data_loss_allowed: bool = None,
                 status: str = None,
                 to_db_cluster_arn: str = None):
        self.from_db_cluster_arn = from_db_cluster_arn
        self.is_data_loss_allowed = is_data_loss_allowed
        self.status = status
        self.to_db_cluster_arn = to_db_cluster_arn

    def to_dict(self):
        return {}

class GlobalClusterMember:
    """
    Represents a member of a global database cluster.

    Attributes:
        db_cluster_arn (str): The ARN of the database cluster.
        global_write_forwarding_status (str): The write forwarding status of the cluster.
        is_writer (bool): Indicates if this cluster is the writer.
        readers (List[str]): List of reader clusters.
        synchronization_status (str): The synchronization status of the cluster.
    """
    def __init__(self, db_cluster_arn: str = None,
                 global_write_forwarding_status: str = None,
                 is_writer: bool = None,
                 readers: List[str] = None,
                 synchronization_status: str = None):
        self.db_cluster_arn = db_cluster_arn
        self.global_write_forwarding_status = global_write_forwarding_status
        self.is_writer = is_writer
        self.readers = readers
        self.synchronization_status = synchronization_status

    def to_dict(self):
        return {}

class GlobalClusterModel:
    
    def __init__(self, database_name:str = None, deletion_protection: bool = None, engine: str = None, engine_lifecycle_support: str = None,
                 engine_version: str = None, failover_state: FailoverState = None, global_cluster_arn: str = None, global_cluster_identifier: str = None,
                 global_cluster_members: List[GlobalClusterMember] = None, global_cluster_resource_id: str = None, status: str = None, storage_encrypted: bool = None):
        
        self.database_name = database_name
        self.deletion_protection = deletion_protection
        self.engine = engine
        self.engine_lifecycle_support = engine_lifecycle_support
        self.engine_version = engine_version
        self.failover_state = failover_state
        self.global_cluster_arn = global_cluster_arn
        self.global_cluster_identifier = global_cluster_identifier
        self.global_cluster_members = global_cluster_members
        self.global_cluster_resource_id = global_cluster_resource_id
        self.status = status
        self.storage_encrypted = storage_encrypted
        
    def to_dict(self):
        return {
            
        }