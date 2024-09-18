from datetime import datetime
from typing import Dict, List, Optional
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
import json

class DBProxyEndpoint:
    pk_column = 'DBProxyEndpointName'
    object_name = 'DBProxyEndpoints'
    foreign_table_name = 'db_proxies' # will change by DBProxy.object_name
    table_structure = f"""
    DBProxyEndpointName VARCHAR(63) PRIMARY KEY NOT NULL,
    DBProxyName VARCHAR(63) NOT NULL,
    TargetRole VARCHAR(10) NOT NULL,
    Tags JSONB DEFAULT '{{}}',
    Status VARCHAR(20) NOT NULL,
    CreatedDate TIMESTAMP NOT NULL,
    Endpoint TEXT NOT NULL,
    IsDefault BOOLEAN NOT NULL,
    FOREIGN KEY (DBProxyName) REFERENCES {foreign_table_name}(DBProxyName)"""

    def __init__(self,  
                 DBProxyEndpointName:str,
                 DBProxyName:str, 
                 TargetRole:Optional[str] = None, 
                 Tags:Optional[List[Dict[str, str]]] = None, 
                 Status:str = 'creating',
                 created_date = datetime.now(),
                 endpoint:str = '',
                 IsDefault:bool = False): 
        self.DBProxyName=DBProxyName
        self.DBProxyEndpointName=DBProxyEndpointName
        self.TargetRole=TargetRole
        self.Tags = Tags
        self.Status = Status
        self.CreatedDate = created_date
        self.Endpoint = endpoint
        self.IsDefault = IsDefault
        self.Status = 'available'

       


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        return ObjectManager.convert_object_attributes_to_dictionary(
            DBProxyEndpointName = self.DBProxyEndpointName,
            DBProxyName = self.DBProxyName,
            TargetRole = self.TargetRole,
            Tags = self.Tags,
            Status = self.Status,
            CreateDate = self.CreatedDate,
            Endpoint = self.Endpoint,
            IsDefault = self.IsDefault
            
        )
    
    