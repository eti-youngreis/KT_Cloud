from typing import Dict, Any, Optional, List
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.DBProxyEndpointModel import DBProxyEndpoint
import json
class DBProxyEndpointManager:
    
    def convert_table_structure_to_columns_arr(table_structure):
        """get a table structure and return arr of the table columns names"""
        columns_arr = [line.split()[0] for line in table_structure.split('\n') if line.strip() and line != 'FOREIGN']
        return columns_arr
    
    def _to_sql(data_dict):
        "convert a dict to values tuple for inserting to sql db"
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values

    
    def __init__(self, object_manager:ObjectManager):
        self.object_manager:ObjectManager = object_manager
        self.object_manager.create_management_table(DBProxyEndpoint.object_name, DBProxyEndpoint.table_structure)
    
    
    def create(self, db_proxy_endpoint_description):
        """insert object to table"""
        values = DBProxyEndpointManager._to_sql(db_proxy_endpoint_description)
        self.object_manager.save_in_memory(DBProxyEndpoint.object_name, values)

    def get(self, name: str):
        """convert data to object"""
        data_mapping = self.select(name)
        return DBProxyEndpoint(**data_mapping)
    
    def select(self, name:Optional[str] = None, columns = ["*"]):
        """select data dict about db proxy endpoint"""
        
        def map_data_to_col_value_dict(data):
            cols = DBProxyEndpointManager.convert_table_structure_to_columns_arr(DBProxyEndpoint.table_structure)
            data_mapping = {col: val for col, val in zip(cols, data)}
            return data_mapping
        
        if name:
            data = self.object_manager.get_from_memory(DBProxyEndpoint.object_name, criteria=f'{DBProxyEndpoint.pk_column} = {name}')
            if data:
                return [map_data_to_col_value_dict(data)]
            else:
                raise ValueError(f"db proxy endpoint with name '{name}' not found")
        else:
            data = self.object_manager.get_all_objects_from_memory(DBProxyEndpoint.object_name)
            return [map_data_to_col_value_dict(row) for row in data]

    
    def is_exists(self, name):
        """check if object exists in table"""
        try:
            self.select(name)
            return True
        except:
            return False

    
    def delete(self, name: str):
        """delete db proxy endpoint from table"""
        self.object_manager.delete_from_memory_by_pk(DBProxyEndpoint.object_name, DBProxyEndpoint.pk_column, name)
    

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
            
    
    def modify(self, name:str,updates:Dict):
        """modify db proxy endpoint in table"""
        criteria=f'{DBProxyEndpoint.pk_column} = {name}'
        self.object_manager.update_in_memory(DBProxyEndpoint.object_name, updates, criteria)
