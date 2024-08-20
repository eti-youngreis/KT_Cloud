from Abc import DBO
from typing import Dict, Optional, List
from Models import DBProxyModel
from DataAccess.ObjectManager import ObjectManager
from exceptions.exception import ParamValidationError
class DBProxyEndpointService(DBO):
    def __init__(self, dal:ObjectManager):
        self.dal = dal

    def create(self, db_proxy_endpoint_name:str, db_proxy_name:str,tags:Dict = {},target_role:str = 'READ_WRITE') -> Dict:
        """create a DBProxyEndpoint."""
        if target_role not in ['READ_WRITE','READ_ONLY']:
            raise ParamValidationError('target role must be in [READ_WRITE,READ_ONLY]')

    def delete(self, db_proxy_endpoint_name:str) -> Dict:
        """elete a DBProxyEndpoint."""
        pass

    def describe(self, db_proxy_name:Optional[str] = None, db_proxy_endpoint_name:Optional[str] = None, filters:Optional[List] = None, marker:Optional[str] = None, max_records:Optional[int] = None):
        """describe DBProxyEndpoints."""
        pass

    def modify(self, db_proxy_endpoint_name:str, new_db_proxy_endpoint_name:Optional[str] = None) -> Dict:
        """modify a DBProxyEndpoint."""
        updates:Dict = {'db_proxy_enpoint_name':db_proxy_endpoint_name}
        current_data = self.dal.select('DBProxyEndpoint', db_proxy_endpoint_name)
        if current_data is None:
            raise ValueError(f"DB Proxy Endpoint '{db_proxy_endpoint_name}' does not exist.")
        
        updated_data = {**current_data, **updates}
        self.dal.update('DBProxyEndpoint', db_proxy_endpoint_name, updated_data)
