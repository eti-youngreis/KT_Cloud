from typing import Dict, Optional, List, Any
from Models.DBProxyEndpointModel import DBProxyEndpoint
from Abc.DBO import DBO
from Validation.GeneralValidations import *
from Validation.DBProxyEndpointValidations import *
from DataAccess.DBProxyEndpointManager import DBProxyEndpointManager

# Exceptions
class ParamValidationFault(Exception):
    pass

class DBProxyNotFoundFault(Exception):
    pass

class DBProxyEndpointNotFoundFault(Exception):
    pass

class DBProxyEndpointAlreadyExistsFault(Exception):
    pass

class DBProxyEndpointQuotaExceededFault(Exception):
    pass

class InvalidDBProxyStateFault(Exception):
    pass

class InvalidDBProxyEndpointStateFault(Exception):
    pass



class DBProxyendpointService(DBO):
    def __init__(self, dal: DBProxyEndpointManager):
        self.dal = dal
   
    

    def create(self, DBProxyName:str, DBProxyEndpointName:str, TargetRole:str = 'READ_WRITE', Tags:Optional[List[Dict[str:str]]] = None, IsDefault:bool = False):
        '''Create a new DBProxy endpoint.'''
        # create object in code
        if not validate_name(DBProxyName):
            raise ParamValidationFault("DBProxyName is not valid")
        if not validate_name(DBProxyEndpointName):
            raise ParamValidationFault("DBProxyEndpointName is not valid") 
        if not validate_target_role:
            raise ParamValidationFault(f"TargetRole {TargetRole} is not valid, must be 'READ_WRITE'|'READ_ONLY'")
        if not validate_tags(Tags):
            raise ParamValidationFault(f"Tags {Tags} are not valid, must be List of dicts [{'Key': 'string','Value': 'string'}]")
        if self.dal.IsDBProxyEndpointExistsInMemory(DBProxyEndpointName):
            raise DBProxyEndpointAlreadyExistsFault(f'db proxy endpoint {DBProxyEndpointName} already exists')
        db_proxy_endpoint:DBProxyEndpoint = DBProxyEndpoint(DBProxyName, DBProxyEndpointName, TargetRole, Tags, IsDefault)
    
        # create physical object as described in task
        
        
        # save in memory using DBClusterManager.createInMemoryDBCluster() function
        self.dal.createInMemoryDBProxyEndpoint(DBProxyEndpointName, db_proxy_endpoint.to_dict())
        return self.describe(DBProxyEndpointName)


    def delete(self, DBProxyEndpointName):
        '''Delete an existing DBProxy endpoint.'''
        if not validate_name(DBProxyEndpointName):
            raise ParamValidationFault("DBProxyEndpointName is not valid") 
        db_proxy_endpoint_description = self.describe(DBProxyEndpointName)
        if not self.dal.IsDBProxyEndpointExistsInMemory(DBProxyEndpointName):
            raise DBProxyEndpointNotFoundFault(f'db proxy endpoint {DBProxyEndpointName} not found')
        self.dal.deleteInMemoryDBProxyEndpoint(DBProxyEndpointName)
        return db_proxy_endpoint_description


    def describe(self,DBProxyName:Optional[str],
                DBProxyEndpointName:Optional[str],
                Filters:Optional[List[Dict[str:Any]]],
                Marker:Optional[str],
                MaxRecords:Optional[int]):
        '''Describe the details of DBProxy endpoint.'''
        if Filters and not check_filters_validation(Filters):
            raise ParamValidationFault("filters are not valid, filters must be list of dicts [{'Name': 'string','Values': ['string',]},]")
        if DBProxyEndpointName and not self.dal.IsDBProxyEndpointExistsInMemory(DBProxyEndpointName):
            raise DBProxyEndpointNotFoundFault(f'db proxy endpoint {DBProxyEndpointName} not found')
        return self.dal.describeDBProxyEndpoint(DBProxyName,
                DBProxyEndpointName,
                Filters,
                Marker,
                MaxRecords)


    def modify(self, DBProxyEndpointName:str, NewDBProxyEndpointName:Optional[str]):
        '''Modify an existing DBProxy endpoint.'''
        if not validate_name(DBProxyEndpointName):
            raise ParamValidationFault("DBProxyEndpointName is not valid") 
        if not validate_name(NewDBProxyEndpointName):
            raise ParamValidationFault("newDBProxyEndpointName is not valid")  
        if not self.dal.IsDBProxyEndpointExistsInMemory(DBProxyEndpointName):
            raise DBProxyEndpointNotFoundFault(f'db proxy endpoint {DBProxyEndpointName} not found')
        if self.dal.IsDBProxyEndpointExistsInMemory(NewDBProxyEndpointName):
            raise DBProxyEndpointAlreadyExistsFault(f'db proxy endpoint name {NewDBProxyEndpointName} already exists')
        db_proxy_endpoint_metadata = self.describe(DBProxyEndpointName)
        endpoint_state = db_proxy_endpoint_metadata['Status']
        if endpoint_state != 'available':
            raise InvalidDBProxyEndpointStateFault(f"db proxy endpoint state is {endpoint_state}. can modify only in available state")
        self.dal.deleteInMemoryDBProxyEndpoint(DBProxyEndpointName)
        self.dal.createInMemoryDBProxyEndpoint(NewDBProxyEndpointName, db_proxy_endpoint_metadata)
        return self.describe(NewDBProxyEndpointName) 


    def get(self):
        '''get code object.'''
        # return real time object
        pass
