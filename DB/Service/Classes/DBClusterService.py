from typing import Dict, Optional
from DataAccess import DataAccessLayer
from Models import DBClusterModel
from Abc import DBO


class DBClusterService(DBO):
        
    def __init__(self, dal:DataAccessLayer):
        self.dal = dal
        self.db_clusters = {}
    
    def get_cluster_by_id(self, cluster_identifier):
        if cluster_identifier not in self.db_clusters:
            return None
        return self.db_clusters[cluster_identifier]
    
    def has_cluster(self, cluster_identifier):
        return cluster_identifier in self.db_clusters

    
    def create(self, **kwargs) -> Dict:
        """Create a new cluster."""
        pass
    
    def modify(self, **kwargs) -> Dict:
        """Modify an existing db cluster"""
        pass

    def describe(self, **kwargs) -> Dict:
        """Retrieve the details of a cluster."""
        pass

    def delete(self, **kwargs):
        """Delete an existing db cluster"""
        pass
