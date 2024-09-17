from DB.NEW_KT_DB.Service.Classes.DBProxyEndpointService import DBProxyEndpointService
from typing import Dict, List, Optional, Any
class DBProxyEndpointController:
    def __init__(self, service: DBProxyEndpointService):
        self.service = service


    def create_db_proxy_endpoint(self, DBProxyName:str, DBProxyEndpointName:str, TargetRole:str = 'READ_WRITE', Tags:Optional[List[Dict[str, str]]] = None):
        self.service.create(DBProxyName, DBProxyEndpointName, TargetRole, Tags)


    def delete_db_proxy_endpoint(self, DBProxyEndpointName:str):
        self.service.delete(DBProxyEndpointName)


    def modify_db_proxy_endpoint(self, DBProxyEndpointName:str, NewDBProxyEndpointName:Optional[str] =  None):
        self.service.modify(DBProxyEndpointName, NewDBProxyEndpointName)
    
    
    def describe_db_proxy_endpoint(self, DBProxyName:Optional[str] = None,
                DBProxyEndpointName:Optional[str] = None,
                Filters:Optional[List[Dict[str, Any]]] = None,
                Marker:Optional[str] = None,
                MaxRecords:Optional[int] = None):
        self.service.describe(DBProxyName,
                DBProxyEndpointName,
                Filters,
                Marker,
                MaxRecords)
    