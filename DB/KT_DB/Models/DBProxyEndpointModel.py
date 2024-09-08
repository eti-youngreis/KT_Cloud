
from typing import Dict
from datetime import datetime
class DBProxyEndpointModel:
    def __init__(self,**kwargs):
        self.db_proxy_endpoint_identifier = kwargs['db_proxy_endpoint_identifier']
        self.db_proxy_identifier = kwargs['db_proxy_identifier']
        self.tags = kwargs.get('tags',{})
        self.target_role = kwargs.get('target_role','READ_WRITE')
        

    def to_dict(self) -> Dict:
        return {
            'db_proxy_endpoint_identifier':self.db_proxy_endpoint_identifier,
            'db_proxy_identifier':self.db_proxy_identifier,
            'tags':self.tags,
            'target_role':self.target_role
        }

