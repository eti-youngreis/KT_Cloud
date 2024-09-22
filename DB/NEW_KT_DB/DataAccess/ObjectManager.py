from typing import Dict, Any, Optional
import json
import sqlite3

from DB.NEW_KT_DB.DataAccess.DBManager import DBManager
 
class ObjectManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.db_manager = DBManager(db_file)


    def create_management_table(self, object_name, table_structure='default', pk_column_data_type='INTEGER'):

        table_name = self._convert_object_name_to_management_table_name(object_name)
        pk_constraint = ' AUTOINCREMENT' if pk_column_data_type == 'INTEGER' else ''

        if table_structure == 'default':
            table_structure = f'object_id {pk_column_data_type} PRIMARY KEY {pk_constraint},type_object TEXT NOT NULL,metadata TEXT NOT NULL'
        self.db_manager.create_table(table_name, table_structure)


    def _insert_object_to_management_table(self, table_name, object_info, columns_to_populate=None):

        if columns_to_populate is None:
            self.db_manager.insert_data_into_table(table_name, object_info)
        else:
            self.db_manager.insert_data_into_table(table_name, object_info, columns_to_populate)


    def _update_object_in_management_table_by_criteria(self, table_name, updates, criteria):
        self.db_manager.update_records_in_table(table_name, updates, criteria)


    def _delete_object_from_management_table(self, table_name, criteria) -> None:
        '''Delete an object from the database.'''
        self.db_manager.delete_data_from_table(table_name, criteria)


    def _convert_object_name_to_management_table_name(self,object_name):
        return f'mng_{object_name}s'


    def save_in_memory(self, object_name, object_info, columns=None):
        # insert object info into management table mng_{object_name}s
        # for exmple: object db_instance will be saved in table mng_db_instances
        table_name = self._convert_object_name_to_management_table_name(object_name)

        if not self._is_management_table_exist(object_name):
            self.create_management_table(object_name)
        
        if columns is None:
            self._insert_object_to_management_table(table_name, object_info)
        else:
            self._insert_object_to_management_table(table_name, object_info, columns)


    def _is_management_table_exist(self, object_name):
            
        table_name = self._convert_object_name_to_management_table_name(object_name)
        return self.db_manager.is_table_exist(table_name)


    def delete_from_memory_by_criteria(self, object_name:str, criteria:str):

        table_name = self._convert_object_name_to_management_table_name(object_name)

        self._delete_object_from_management_table(table_name, criteria)


    def delete_from_memory_by_pk(self, object_name:str, pk_column:str, pk_value:str):

        criteria = f"{pk_column} = '{pk_value}'"

        table_name = self._convert_object_name_to_management_table_name(object_name)

        self._delete_object_from_management_table(table_name, criteria)


    def update_in_memory(self, object_name, updates, criteria):

        table_name = self._convert_object_name_to_management_table_name(object_name)
        self._update_object_in_management_table_by_criteria(table_name, updates, criteria)


    def get_from_memory(self, object_name, columns=None, criteria=None):
        """get records from memory by criteria or id"""
        table_name = self._convert_object_name_to_management_table_name(object_name)

        if columns is None and criteria is None:
            return self.db_manager.get_data_from_table(table_name)
        elif columns is None:
            return self.db_manager.get_data_from_table(table_name, criteria=criteria)
        elif criteria is None:
            return self.db_manager.get_data_from_table(table_name, columns)
        else:
            return self.db_manager.get_data_from_table(table_name, columns, criteria)


    def get_all_objects_from_memory(self, object_name):
        table_name = self._convert_object_name_to_management_table_name(object_name)
        return self.db_manager.get_all_data_from_table(table_name)


    @staticmethod
    def convert_object_attributes_to_dictionary(**kwargs):
        dict = {}
        for key, value in kwargs.items():
            dict[key] = value
        return dict