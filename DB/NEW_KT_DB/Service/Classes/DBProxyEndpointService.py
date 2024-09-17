from typing import Dict, Optional, List, Any
from datetime import datetime
import json
from DB.NEW_KT_DB.Models.DBProxyEndpointModel import DBProxyEndpoint
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from DB.NEW_KT_DB.Validation.GeneralValidations import *
from DB.NEW_KT_DB.Validation.DBProxyEndpointValidations import *
from DB.NEW_KT_DB.DataAccess.DBProxyEndpointManager import DBProxyEndpointManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

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



class DBProxyEndpointService(DBO):
    def __init__(self, dal: DBProxyEndpointManager, storage:StorageManager, db_proxy_service):
        self.dal:DBProxyEndpointManager = dal
        self.storage:StorageManager = storage
   
    def _convert_endpoint_name_to_endpoint_file_name(self, DBProxyEndpointName:str):
        return "endpoint_"+DBProxyEndpointName
    
    def _get_json(self, object_dict):
        def custom_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        object_dict = {k: v for k, v in object_dict.items() if v is not None}
        object_json = json.dumps(object_dict, default=custom_serializer)
        return object_json

    # def _load_json(json_str):
    #     def custom_deserializer(obj):
    #         if 'datetime' in obj:
    #             return datetime.fromisoformat(obj['datetime'])
    #         return obj
    #     json_dict = json.loads(json_str, object_hook=custom_deserializer)
    #     return json_dict


    def create(self, DBProxyName:str, DBProxyEndpointName:str, TargetRole:str = 'READ_WRITE', Tags:Optional[List[Dict[str, str]]] = None, IsDefault:bool = False):
        '''Create a new DBProxy endpoint.'''
        # Validations
        if not validate_name(DBProxyName):
            raise ParamValidationFault("DBProxyName is not valid")
        if not validate_name(DBProxyEndpointName):
            raise ParamValidationFault("DBProxyEndpointName is not valid") 
        if not validate_target_role:
            raise ParamValidationFault(f"TargetRole {TargetRole} is not valid, must be 'READ_WRITE'|'READ_ONLY'")
        if not validate_tags(Tags):
            raise ParamValidationFault(f"Tags {Tags} are not valid, must be List of dicts [{'Key': 'string','Value': 'string'}]")
        if self.dal.is_exists(DBProxyEndpointName):
            raise DBProxyEndpointAlreadyExistsFault(f'db proxy endpoint {DBProxyEndpointName} already exists')
        
        # create object
        db_proxy_endpoint:DBProxyEndpoint = DBProxyEndpoint(DBProxyName, DBProxyEndpointName, TargetRole, Tags, IsDefault)
    
        # create physical object as described in task
        file_name = self._convert_endpoint_name_to_endpoint_file_name(DBProxyEndpointName)
        content = self._get_json(db_proxy_endpoint.to_dict())
        self.storage.create_file(file_name, content)
        
        # save in memory 
        self.dal.create(db_proxy_endpoint)
        return self.describe(DBProxyEndpointName=DBProxyEndpointName)


    def delete(self, DBProxyEndpointName:str):
        '''Delete an existing DBProxy endpoint.'''
        
        # Validations
        if not validate_name(DBProxyEndpointName):
            raise ParamValidationFault("DBProxyEndpointName is not valid") 
        db_proxy_endpoint_description = self.describe(DBProxyEndpointName)
        if not self.dal.is_exists(DBProxyEndpointName):
            raise DBProxyEndpointNotFoundFault(f'db proxy endpoint {DBProxyEndpointName} not found')
        
        # Delete phisical object
        file_name = self._convert_endpoint_name_to_endpoint_file_name(DBProxyEndpointName)
        self.storage.delete_file(file_name)
        
        # Delete from table
        self.dal.delete(DBProxyEndpointName)
        return db_proxy_endpoint_description


    def describe(self,
                DBProxyEndpointName:Optional[str] = None,
                DBProxyName:Optional[str] = None,
                Filters:Optional[List[Dict[str, Any]]] = None,
                Marker:Optional[str] = None,
                MaxRecords:Optional[int] = None):
        '''Describe the details of DBProxy endpoint.'''
        
        # Validations
        if Filters and not check_filters_validation(Filters):
            raise ParamValidationFault("filters are not valid, filters must be list of dicts [{'Name': 'string','Values': ['string',]},]")
        if DBProxyEndpointName and not self.dal.is_exists(DBProxyEndpointName):
            raise DBProxyEndpointNotFoundFault(f'db proxy endpoint {DBProxyEndpointName} not found')
        
        # Describe in query
        return self.dal.describe(
                DBProxyEndpointName,
                Filters)

    


    def modify(self, DBProxyEndpointName:str, NewDBProxyEndpointName:Optional[str] =  None):
        '''Modify an existing DBProxy endpoint.'''
        
        # Validations
        if not validate_name(DBProxyEndpointName):
            raise ParamValidationFault("DBProxyEndpointName is not valid") 
        
        if not self.dal.IsDBProxyEndpointExistsInMemory(DBProxyEndpointName):
            raise DBProxyEndpointNotFoundFault(f'db proxy endpoint {DBProxyEndpointName} not found')
        
        
        # If need changes:
        if NewDBProxyEndpointName:
            
            # Validations
            
            if not validate_name(NewDBProxyEndpointName):
                raise ParamValidationFault("newDBProxyEndpointName is not valid") 
            
            if self.dal.IsDBProxyEndpointExistsInMemory(NewDBProxyEndpointName):
                raise DBProxyEndpointAlreadyExistsFault(f'db proxy endpoint name {NewDBProxyEndpointName} already exists') 
            
            # Check if state is valid
            endpoint_state = self.dal.select(DBProxyEndpointName, ['Status'])['Status']
            if endpoint_state != 'available':
                raise InvalidDBProxyEndpointStateFault(f"db proxy endpoint state is {endpoint_state}. can modify only in available state")
            
            # Change phisical object
            old_file_name = self._convert_endpoint_name_to_endpoint_file_name(DBProxyEndpointName)
            new_file_name = self._convert_endpoint_name_to_endpoint_file_name(NewDBProxyEndpointName)
            self.storage.rename_file(old_file_name, new_file_name)
            
            # Change in memory
            db_proxy_endpoint = self.dal.get(DBProxyEndpointName)
            db_proxy_endpoint.DBProxyEndpointName = NewDBProxyEndpointName
            self.dal.delete(DBProxyEndpointName)
            self.dal.create(db_proxy_endpoint)
            DBProxyEndpointName = NewDBProxyEndpointName
        return self.describe(DBProxyEndpointName) 


    def get(self):
        '''get code object.'''
        # return real time object
        pass
