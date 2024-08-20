from Abc import DBO
from typing import Dict, Optional, List
from Models.DBProxyModel import DBProxyModel
from DataAccess.ObjectManager import ObjectManager
from Validation.Validation import check_filters_validation
from exceptions.exception import ParamValidationError
from exceptions.db_proxy_exception import DBProxiNotExistsError

class DBProxyService(DBO):
    def __init__(self, dal:ObjectManager):
        self.db_proxies = {}
        self.dal = dal
        self.object_type = 'DBProxy'

    def create(self, db_proxy_name:str, auth:Dict, require_TLS:bool = False,
    idle_client_timeout:int=123,
    debug_logging:bool=False,tags:Dict = {}) -> Dict:
        """create a DBProxy."""
        db_proxy = DBProxyModel(db_proxy_name, auth, require_TLS, idle_client_timeout, debug_logging, tags)
        self.db_proxies[db_proxy_name] = db_proxy
        self.dal.create(self.object_type, db_proxy.to_dict())

    def delete(self, db_proxy_name:str) -> Dict:
        """delete a DBProxy."""
        if db_proxy_name not in self.db_proxies:
            raise DBProxiNotExistsError(f'db with key: {db_proxy_name} does not exist')
        self.dal.delete(db_proxy_name)

    def describe(self, db_proxy_name:Optional[str] = None, filters:Optional[str] = None, marker:Optional[str] = None, max_records:Optional[int] = None) -> Dict:
        """describe DBProxies."""
        if db_proxy_name and db_proxy_name not in self.db_proxies:
            raise DBProxiNotExistsError(f'db with key: {db_proxy_name} does not exist')
        if filters and not check_filters_validation(filters):
            raise ParamValidationError('filters must be a list of dicts, with this structure: {Name:str, Values:list(str)}')
        

    def modify(self, db_proxy_name:str, new_db_proxy_name:Optional[str]=  None, require_TLS:Optional[bool] = None,
    idle_client_timeout:Optional[int] = None,
    debug_logging:Optional[bool] = None) -> Dict:
        """modify a DBProxy."""
        if db_proxy_name not in self.db_proxies:
            raise DBProxiNotExistsError(f'db with key: {db_proxy_name} does not exist')
        db_proxy = self.db_proxies[db_proxy_name]
        db_proxy.modify(new_db_proxy_name,require_TLS, idle_client_timeout, debug_logging)
            
        
    def register_db_proxy_targets():
        pass
    
    def deregister_db_proxy_targets():
        pass
        
