from Service import DBProxyService
from typing import Dict, Optional, List
class DBProxyController:
    def __init__(self, service: DBProxyService):
        self.service = service

    def create_db_proxy(self, db_proxy_name:str, auth:Dict, require_TLS:bool = False,
    idle_client_timeout:int=123,
    debug_logging:bool=False,tags:Dict = {}) -> Dict:
        """create a db proxy"""
        return self.service.create(db_proxy_name, auth, require_TLS,idle_client_timeout,debug_logging,tags)
    
    def delete_db_proxy(self,db_proxy_name:str) -> Dict:
        """delete a db proxy"""
        return self.service.delete(db_proxy_name)
    
    def describe_db_proxies(self,db_proxy_name:Optional[str] = None, filters:Optional[str] = None, marker:Optional[str] = None, max_records:Optional[int] = None) -> Dict:
        """describe db proxies"""
        return self.service.describe(db_proxy_name, filters, marker, max_records)
        
    def modify_db_proxy(self, db_proxy_name:str, new_db_proxy_name:Optional[str]=  None, require_TLS:Optional[bool] = None,
    idle_client_timeout:Optional[int] = None,
    debug_logging:Optional[bool] = None) -> Dict:
        """modify a db proxy"""
        return self.service.modify(db_proxy_name, new_db_proxy_name, require_TLS,idle_client_timeout,debug_logging)
        
