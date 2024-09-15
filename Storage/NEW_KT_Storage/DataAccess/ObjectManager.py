from typing import Dict, Any
import json
import sqlite3
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from StorageManager import StorageManager

class ObjectManager:
    def __init__(self, db_file: str,type, storage_path=None):
        """Initialize ObjectManager with the database connection."""
        self.object_manager = ObjectManager(db_file)
        self.storage_manager = StorageManager(storage_path)
        self.object_name = type

    # for outer use:
    def save_in_memory(self, object):
       
        # insert object info into management table mng_{object_name}s
        # for exmple: object db_instance will be saved in table mng_db_instances
        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)

        if not self.object_manager.is_management_table_exist(table_name):
            self.object_manager.create_management_table(table_name)
        
        self.object_manager.insert_object_to_management_table(table_name, object)

    
    def delete_from_memory(self,object, criteria='default'):
        
        # if criteria not sent- use PK for deletion
        if criteria == 'default':
            criteria = f'{object.pk_column} = {object.pk_value}'
        
        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)
        
        self.object_manager.delete_data_from_table(table_name, criteria)


    def update_in_memory(self,object, updates, criteria='default'):
        
        # if criteria not sent- use PK for deletion
        if criteria == 'default':
            criteria = f'{object.pk_column} = {object.pk_value}'

        table_name = self.object_manager.convert_object_name_to_management_table_name(self.object_name)

        self.object_manager.update_object_in_management_table_by_criteria(table_name, updates, criteria)

    def get_from_memory(self, table_name, object_id):
        self.object_manager.get_object_from_management_table(table_name,object_id)

    def get_data_from_memory_db(self, table_name):
        self.object_manager.get_data_from_memory(table_name=table_name)
        
    def convert_object_attributes_to_dictionary(**kwargs):

        dict = {}

        for key, value in kwargs.items():
            dict[key] = value
    
        return dict
