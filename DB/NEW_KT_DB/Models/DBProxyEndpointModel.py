from datetime import datetime
from typing import Dict, List, Optional
from DataAccess.ObjectManager import ObjectManager

class DBProxyEndpoint:

    def __init__(self, DBProxyName:str, DBProxyEndpointName:str, TargetRole:Optional[str] = None, Tags:Optional[List[Dict[str:str]]] = None, IsDefault:bool = False): 
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