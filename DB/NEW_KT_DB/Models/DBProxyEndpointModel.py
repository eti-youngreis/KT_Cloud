from datetime import datetime
from typing import Dict, List, Optional
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
import json

class DBProxyEndpoint:
    pk_column = 'DBProxyEndpointName'
    object_name = 'DBProxyEndpoints'
    foreign_table_name = 'DBProxies'
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

    def __init__(self, DBProxyName:str, DBProxyEndpointName:str, TargetRole:Optional[str] = None, Tags:Optional[List[Dict[str, str]]] = None, IsDefault:bool = False): 
        self.DBProxyName=DBProxyName
        self.DBProxyEndpointName=DBProxyEndpointName
        self.TargetRole=TargetRole
        self.Tags = Tags
        self.Status = 'creating'
        self.CreatedDate = datetime.now()
        self.Endpoint = ''
        self.IsDefault = IsDefault

       


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        return ObjectManager.convert_object_attributes_to_dictionary(
            DBProxyName = self.DBProxyName,
            DBProxyEndpointName = self.DBProxyEndpointName,
            Status = self.Status,
            TargetRole = self.TargetRole,
            Tags = self.Tags,
            Endpoint = self.Endpoint,
            CreateDate = self.CreatedDate,
            IsDefault = self.IsDefault
            
        )
    
    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values