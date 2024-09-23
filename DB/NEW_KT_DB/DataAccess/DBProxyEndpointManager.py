from typing import Dict, Any, Optional, List
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.DBProxyEndpointModel import DBProxyEndpoint
import json
import ast
class DBProxyEndpointManager:
    
    # Static functions
    @staticmethod
    def convert_table_structure_to_columns_arr(table_structure):
        """get a table structure and return arr of the table columns names"""
        columns_arr = [line.split()[0] for line in table_structure.split('\n') if line.strip() and line.split()[0] != 'FOREIGN']
        return columns_arr
    
    
    @staticmethod
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
        data_mapping = self.get_object_attributes_dict(name)[0]
        return DBProxyEndpoint(**data_mapping)
    
    
    def get_object_attributes_dict(self, name:Optional[str] = None, columns:Optional[List[str]] =None):
        """Selects data attributes of DBProxyEndpoint objects.

            Args:
                name: Optional. The unique name of the object to select. If not provided, all objects are selected.
                columns: Optional. The list of columns to retrieve. If not provided, all columns are selected.

            Returns:
                A list of dictionaries where keys are column names and values are data values.
        
            Raises:
                ValueError: If no data is found based on the criteria
            """
        
        def map_query_data_to_col_value_dict(data, cols: Optional[List[str]] = None):
            """
            Help function. Maps the given data to a dictionary where keys are column names and values are data values.

            Args:
                data: The data values to be mapped in tuple as they ware returned from query.
                cols: Optional. The list of column names. If not provided, all columns from the default table structure will be used.

            Returns:
                A dictionary mapping column names to data values.
            """
            if not cols:
                cols = DBProxyEndpointManager.convert_table_structure_to_columns_arr(DBProxyEndpoint.table_structure)
            data_mapping = {col: ast.literal_eval(val) if (isinstance(val, str) and val[0] == '[' and val[-1] == ']') else val for col, val in zip(cols, data)}
            return data_mapping
        
        # cast columns arr to str for query
        if columns:
            columns:str = "".join(columns)
        
        # Select one object by its unique name
        if name:
            error = f"db proxy endpoint with name '{name}' not found"
            data = self.object_manager.get_from_memory(DBProxyEndpoint.object_name, columns, criteria=f"{DBProxyEndpoint.pk_column} = '{name}'")
        # Select all objects
        else:
            error = f"there is no objects in table of {DBProxyEndpoint.object_name}"
            data = self.object_manager.get_from_memory(DBProxyEndpoint.object_name, columns)
        if data:
            
            # convert columns str to arr in back for function map_data_to_col_value_dict
            if columns:
                columns = columns.split(",")
            data = [map_query_data_to_col_value_dict(row, columns) for row in data]
            return data
            
        else:
            raise ValueError(error)


    def is_exists(self, name):
        """check if object exists in table"""
        try:
            self.get_object_attributes_dict(name)
            return True
        except:
            return False

    
    def delete(self, name: str):
        """delete db proxy endpoint from table"""
        self.object_manager.delete_from_memory_by_pk(DBProxyEndpoint.object_name, DBProxyEndpoint.pk_column, name)
    

    def describe(self, name: Optional[str] = None, Filters:Optional[List[Dict[str, Any]]] = None):
        """describe db proxy endpoint""" 
        if name:
            description = self.get_object_attributes_dict(name)
        else:
            description = self.get_object_attributes_dict()
        # If there are filters return only objects that in conditions of all filters
        if Filters:
            for Filter in Filters:
                description = [obj for obj in description if all(col not in Filter['Name'] or obj[col] in Filter['Values'] for col in obj.keys())]

        return {DBProxyEndpoint.object_name: description}
            
    
    def modify(self, name:str,updates:Dict):
        """modify db proxy endpoint in table"""
        criteria=f'{DBProxyEndpoint.pk_column} = "{name}"'
        set_clause = ', '.join([f"{key} = '{value}'" for key, value in updates.items()])
        self.object_manager.update_in_memory(DBProxyEndpoint.object_name, set_clause, criteria)
