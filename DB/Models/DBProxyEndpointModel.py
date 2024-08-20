
from typing import Dict
from datetime import datetime
class DBProxyEndpointModel:
    def __init__(self, db_proxy_endpoint_name:str, db_proxy_name:str,tags:Dict = {}, target_role:str = 'READ_WRITE'):
        self.db_proxy_endpoint_name = db_proxy_endpoint_name
        self.db_proxy_name = db_proxy_name
        self.tags = tags
        self.target_role = target_role
        self.create_date = datetime.now()
        

    def to_dict(self) -> Dict:
        return {
            'db_proxy_endpoint_name':self.db_proxy_endpoint_name,
            'db_proxy_name':self.db_proxy_name,
            'tags':self.tags,
            'target_role':self.target_role,
            'create_date': self.create_date
        }

