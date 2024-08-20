from Service import DBProxyService
from typing import Dict, Optional, List
class DBProxyEndpointController:
    def __init__(self, service: DBProxyService):
        self.service = service

    def create_db_proxy_endpoint(self, db_proxy_endpoint_name:str, db_proxy_name:str,tags:Dict = {},target_role:str = 'READ_WRITE') -> Dict:
        return self.service.create(db_proxy_endpoint_name, db_proxy_name,tags,target_role)

    def delete_db_proxy_endpoint(self, db_proxy_endpoint_name:str) -> Dict:
        return self.service.delete(db_proxy_endpoint_name)

    def describe_db_proxy_enpoints(self, db_proxy_name:Optional[str] = None, db_proxy_endpoint_name:Optional[str] = None, filters:Optional[List] = None, marker:Optional[str] = None, max_records:Optional[int] = None):
        return self.service.describe(db_proxy_name, db_proxy_endpoint_name, filters, marker, max_records)

    def modify_db_proxy_endpoint(self, db_proxy_endpoint_name:str, new_db_proxy_endpoint_name:Optional[str] = None) -> Dict:
        return self.service.modify(db_proxy_endpoint_name, new_db_proxy_endpoint_name)

        
