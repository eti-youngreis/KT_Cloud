from DB.NEW_KT_DB.Service.Classes.DBProxyEndpointService import DBProxyEndpointService
from typing import Dict, List, Optional, Any


class DBProxyEndpointController:
    def __init__(self, service: DBProxyEndpointService):
        self.service = service


    def create_db_proxy_endpoint(self, DBProxyName:str, DBProxyEndpointName:str, VpcSubnetIds:List[str],
                VpcSecurityGroupIds:Optional[List[str]] = None, TargetRole:str = 'READ_WRITE', Tags:Optional[List[Dict[str, str]]] = None):
        """Create a db proxy endpoint"""
        return self.service.create(DBProxyName, DBProxyEndpointName,VpcSubnetIds,
                VpcSecurityGroupIds, TargetRole, Tags)


    def delete_db_proxy_endpoint(self, DBProxyEndpointName:str):
        """Delete a db proxy endpoint"""
        return self.service.delete(DBProxyEndpointName)


    def modify_db_proxy_endpoint(self, DBProxyEndpointName:str, NewDBProxyEndpointName:Optional[str] =  None, 
                                 VpcSubnetIds:Optional[List[str]] =  None):
        """Modify a db proxy endpoint"""
        return self.service.modify(DBProxyEndpointName, NewDBProxyEndpointName, VpcSubnetIds)
    
    
    def describe_db_proxy_endpoint(self, 
                DBProxyEndpointName:Optional[str] = None,
                DBProxyName:Optional[str] = None,
                Filters:Optional[List[Dict[str, Any]]] = None,
                Marker:Optional[str] = None,
                MaxRecords:Optional[int] = None):
        """Describe a db proxy endpoint"""
        return self.service.describe(
                DBProxyEndpointName,
                DBProxyName,
                Filters,
                Marker,
                MaxRecords)
    