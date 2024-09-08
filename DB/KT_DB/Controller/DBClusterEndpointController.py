from typing import Optional,Dict
from Service import DBClusterEndpointService
class DBClusterEndpointController:
    def __init__(self, service: DBClusterEndpointService ):
        self.service = service

    def create_db_cluster_endpoint(self, cluster_identifier: str, endpoint_identifier: str, endpoint_type: Optional[str]='READER', static_members: Optional[str]=None, excluded_members: Optional[str] = None):
        self.service.create(cluster_identifier, endpoint_identifier, endpoint_type, static_members, excluded_members)

    def delete_db_cluster_endpoint(self, endpoint_identifier: str):
        self.service.delete(endpoint_identifier)

    def describe_db_cluster_endpoints(self, endpoint_identifier: str)-> Dict:
        return self.service.describe(endpoint_identifier)
    
    def modify_db_cluster_endpoint(self, endpoint_identifier: str, endpoint_type: Optional[str]='READER', static_members: Optional[str]=None, excluded_members: Optional[str] = None):
        updates={
            'endpoint_type': endpoint_type,
            'static_members': static_members,
            'excluded_members': excluded_members
        }
        self.service.modify(endpoint_identifier,updates)