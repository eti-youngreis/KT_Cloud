from typing import Dict, Any, Optional, List
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.DBProxyEndpointModel import DBProxyEndpoint

class DBProxyEndpointManager:
    
    def __init__(self, object_manager):
        self.object_manager:ObjectManager = object_manager
        self.object_manager._create_management_table(DBProxyEndpoint.object_name, DBProxyEndpoint.table_structure)

    def create(self, db_proxy_endpoint: DBProxyEndpoint):
        """insert object to table"""
        self.object_manager.save_in_memory(DBProxyEndpoint.object_name, db_proxy_endpoint)

    def get(self, name: str):
        """convert data to object"""
        data = self.object_manager.get_from_memory_by_id(DBProxyEndpoint.pk_column, DBProxyEndpoint.object_name, name)
        if data:
            data_mapping = {'DBProxyEndpointName':name}
            for key, value in data[name].items():
                data_mapping[key] = value 
            return DBProxyEndpoint(**data_mapping)
        else:
            raise ValueError(f"db proxy endpoint with name '{name}' not found")
    
    def select(self, name:Optional[str] = None, columns = ["*"]):
        """select data dict about db proxy endpoint"""
        if name:
            data = self.object_manager.get_from_memory_by_id(DBProxyEndpoint.pk_column, DBProxyEndpoint.object_name, name, columns)
            if data:
                data_to_return = [{col:data[col] for col in columns}]
                data_to_return[DBProxyEndpoint.pk_column] = name
                return data_to_return
                
            else:
                raise ValueError(f"db proxy endpoint with name '{name}' not found")
        else:
            data = self.object_manager._get_objects_from_management_table_by_criteria(DBProxyEndpoint.object_name, columns)
            return [{col: data[data_row][col] for col in columns} if columns != ["*"] else {col: data[data_row][col] for col in data[data_row].keys()} for data_row in data.keys()]

    
    def is_exists(self, name):
        """check if object exists in table"""
        try:
            self.select(name)
            return True
        except:
            return False

    
    def delete(self, name: str):
        """delete db proxy endpoint from table"""
        self.object_manager.delete_from_memory_by_id(DBProxyEndpoint.pk_column, name, DBProxyEndpoint.object_name)
    

    def describe(self, name: Optional[str] = None, Filters:Optional[List[Dict[str, Any]]] = None):
        """describe db proxy endpoint""" 
        if name:
            description = self.select(name)
        else:
            description = self.select()
        # If there are filters return only objects that in conditions of all filters
        if Filters:
            description = [obj for obj in description if [col for col in obj.keys() if col not in Filters or obj[col] in Filters[col]] != []]
        return {DBProxyEndpoint.object_name: description}
            
    
    def modify(self, db_proxy_endpoint: DBProxyEndpoint):
        """modify db proxy endpoint in table"""
        updates = db_proxy_endpoint.to_dict()
        del updates['DBProxyEndpointName']
        self.object_manager.update_in_memory_by_id(DBProxyEndpoint.pk_column, DBProxyEndpoint.object_name, updates, DBProxyEndpoint.db_subnet_group_name)
