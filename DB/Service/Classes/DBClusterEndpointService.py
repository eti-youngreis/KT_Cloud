from Abc import DBO
from typing import Optional, List
from DB.Validation.ValiditionDBClusterEndpoint import is_valid_identifier
from Models import DBClusterEndpointModel
from DataAccess import DataAccessLayer
from typing import Dict, Optional

class DBClusterEndpointService(DBO):
    def __init__(self, dal: DataAccessLayer):
        self.dal = dal


    def create(self, cluster_identifier: str, endpoint_identifier: str, endpoint_type: str, 
               static_members:  Optional[List[str]] = None, excluded_members:  Optional[List[str]] = None):
        """Create a new DB Cluster Endpoint."""
        if not is_valid_identifier(cluster_identifier):
            raise ValueError(f"Invalid cluster_identifier: {cluster_identifier}")
        if not is_valid_identifier(endpoint_identifier):
            raise ValueError(f"Invalid endpoint_identifier: {endpoint_identifier}")
        if excluded_members is None:
            excluded_members = []
        if static_members is None:
            static_members = [inst.db_instance_identifier for inst in self.instances if inst.db_instance_identifier
                              not in excluded_members]
        endpoint = DBClusterEndpointModel(cluster_identifier, endpoint_identifier, endpoint_type, static_members, excluded_members)
        self.dal.insert('DBClusterEndpoint', endpoint.to_dict())

    def delete(self, endpoint_identifier: str):
        """Delete an existing DB Cluster Endpoint."""
        if not self.dal.exists('DBClusterEndpoint', endpoint_identifier):
            raise ValueError(f"DB Cluster Endpoint '{endpoint_identifier}' does not exist.")
        self.dal.delete('DBClusterEndpoint', endpoint_identifier)

    def describe(self, endpoint_identifier: str) -> Dict:
        """Retrieve the details of a DB Cluster Endpoint."""
        data = self.dal.select('DBClusterEndpoint', endpoint_identifier)
        if data is None:
            raise ValueError(f"DB Cluster Endpoint '{endpoint_identifier}' does not exist.")
        return data

    def modify(self, endpoint_identifier: str, updates: Dict):
        """Modify an existing DB Cluster Endpoint."""
        if not self.dal.exists('DBClusterEndpoint', endpoint_identifier):
            raise ValueError(f"DB Cluster Endpoint '{endpoint_identifier}' does not exist.")
        
        current_data = self.dal.select('DBClusterEndpoint', endpoint_identifier)
        if current_data is None:
            raise ValueError(f"DB Cluster Endpoint '{endpoint_identifier}' does not exist.")
        
        updated_data = {**current_data, **updates}
        self.dal.update('DBClusterEndpoint', endpoint_identifier, updated_data)

    