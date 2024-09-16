from typing import Dict, Any, Optional
import json
import sqlite3
from DBManager import DBManager

class ObjectManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.db_manager = DBManager(db_file)


    # for internal use only:

    # Riki7649255 based on rachel-8511
    def create_management_table(self, table_name, table_structure='object_id INTEGER PRIMARY KEY AUTOINCREMENT,type_object TEXT NOT NULL,metadata TEXT NOT NULL'):
        self.db_manager.create_table(table_name, table_structure)
    
    def create_management_table_with_str_id(self, table_name, table_structure='object_id TEXT NOT NULL PRIMARY KEY ,metadata TEXT NOT NULL'):
        self.db_manager.create_table(table_name, table_structure)

    
    # Riki7649255 based on saraNoigershel
    def insert_object_to_management_table(self, table_name, object_metadata):
        self.db_manager.insert_data_into_table(table_name, ['metadata'], (object_metadata))

    def insert_object_to_management_table_with_str_id(self, table_name, id, metadata):
        self.db_manager.insert_data_into_table(table_name, ['object_id','metadata'],(id, metadata))


    # Riki7649255 based on rachel-8511
    def update_object_in_management_table_by_criteria(self, table_name, updates, criteria):
        self.db_manager.update_records_in_table(table_name, updates, criteria)
    
    def update_object_in_management_table_by_id(self, table_name, object_id, updates):
        self.db_manager.update_records_in_table(table_name, updates, f'object_id = {object_id}')



    # rachel-8511, Riki7649255
    def get_object_from_management_table(self, table_name, object_id: int, columns = ["*"]) -> Dict[str, Any]:
        '''Retrieve an object from the database.'''
        result = self.db_manager.select_and_return_records_from_table(table_name, columns, criteria= f'object_id = {object_id}')
        if result:
            return result[object_id]
        else:
            raise FileNotFoundError(f'Object with ID {object_id} not found.')
    
    def get_objects_from_management_table_by_criteria(self, object_id: int, columns = ["*"], criteria:Optional[str] = None) -> Dict:
        '''Retrieve an object from the database.'''
        result = self.db_manager.select_and_return_records_from_table(self.table_name, columns, criteria)
        if result:
            return result
        else:
            raise FileNotFoundError(f'Objects with criteria {criteria} not found.')


    # rachel-8511, ShaniStrassProg, Riki7649255
    def delete_object_from_management_table(self, table_name, criteria) -> None:
        '''Delete an object from the database.'''
        self.db_manager.delete_data_from_table(table_name, criteria)
    
    def delete_object_from_management_table_by_id(self, table_name, object_id) -> None:
        '''Delete an object from the database.'''
        self.db_manager.delete_data_from_table(table_name, criteria= f'object_id = {object_id}')


    # rachel-8511, ShaniStrassProg is it needed?
    # def get_all_objects(self) -> Dict[int, Dict[str, Any]]:
    #     '''Retrieve all objects from the database.'''
    #     return self.db_manager.select(self.table_name, ['object_id', 'type_object', 'metadata'])


    # rachel-8511 is it needed?
    # def describe_table(self) -> Dict[str, str]:
    #     '''Describe the schema of the table.'''
    #     return self.db_manager.describe(self.table_name)


    def convert_object_name_to_management_table_name(object_name):
        return f'mng_{object_name}s'


    def is_management_table_exist(self, table_name):
        # Check if table exists by querying the sqlite_master table
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        return self.db_manager.execute_query_with_single_result(query)


    # for outer use:
    def save_in_memory(self, object_name:str, metadata:Dict[str:Any], object_id:Optional[str] = None):

        # insert object info into management table mng_{object_name}s
        # for exmple: object db_instance will be saved in table mng_db_instances
        table_name = self.convert_object_name_to_management_table_name(object_name)

        if not self.is_management_table_exist(table_name):
            self.create_management_table(table_name)

        if object_id:
            self.insert_object_to_management_table_with_str_id(table_name, object_id, metadata)

        else:
            self.insert_object_to_management_table(table_name, metadata)

    
    def delete_from_memory(self,object_name:str, criteria='default', object_id:Optional[str] =  None):

        # if criteria not sent- use PK for deletion
        if criteria == 'default':
            if not object_id:
                raise ValueError('must be or criteria or object id')
            criteria = f'object_id = {object_id}'

        table_name = self.convert_object_name_to_management_table_name(object_name)

        self.delete_object_from_management_table(table_name, criteria)


    def update_in_memory(self, object_name, updates, criteria='default', object_id:Optional[str] =  None):

        # if criteria not sent- use PK for deletion
        if criteria == 'default':
            if not object_id:
                raise ValueError('must be or criteria or object id')
            criteria = f'object_id = {object_id}'

        table_name = self.convert_object_name_to_management_table_name(object_name)
        self.update_object_in_management_table_by_criteria(table_name, updates, criteria)

    
    def get_from_memory(self, object_name, columns = ["*"], object_id = None, criteria = None):
        """get records from memory by criteria or id"""
        table_name = self.convert_object_name_to_management_table_name(object_name)
        if object_id:
            criteria = f'object_id = {object_id}'
        self.get_objects_from_management_table_by_criteria(table_name, columns, criteria)


    def convert_object_attributes_to_dictionary(**kwargs):

        dict = {}

        for key, value in kwargs.items():
            dict[key] = value
    
        return dict
        
def is_exists(self, object):
        table_name = convert_object_name_to_management_table_name(object.object_name)
        try:
            query=f'select * from {table_name} where {object.pk_column} = {object.pk_value}'
            result=self.db_manager.execute_query_with_single_result(query)
            if result is None:
                return False
            return True 
        except sqlite3.OperationalError as e:
            return False 
