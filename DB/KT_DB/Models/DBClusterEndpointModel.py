
from typing import Dict ,Optional, List
class DBClusterEndpointModel:
    def __init__(self, cluster_identifier: str, endpoint_identifier: str, endpoint_type: str, static_members: Optional[List[str]]=None, excluded_members: Optional[List[str]] = None):
        self.endpoint_identifier = endpoint_identifier
        self.cluster_identifier = cluster_identifier
        self.endpoint_type = endpoint_type
        self.static_members = static_members
        self.excluded_members = excluded_members
        self.counter = 0

    def to_dict(self) -> Dict:
        return {
            'endpoint_identifier': self.endpoint_identifier,
            'cluster_identifier': self.cluster_identifier,
            'endpoint_type': self.endpoint_type,
            'static_members': self.static_members,
            'excluded_members': self.excluded_members
        }
        