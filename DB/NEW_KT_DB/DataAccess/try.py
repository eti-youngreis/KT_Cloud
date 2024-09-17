from typing import Dict, Any, Optional
import json
import sqlite3
from DB.NEW_KT_DB.DataAccess.DBManager import DBManager
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
        
    def insert_object_to_management_table(self, table_name, object):
        self.db_manager.insert_data_into_table(table_name, object)
        
    def update_object_in_management_table_by_id(self, table_name, object_id, updates):
            self.db_manager.update_records_in_table(table_name, updates, f'object_id = {object_id}')
    # Riki7649255 based on rachel-8511
    def update_object_in_management_table_by_criteria(self, table_name, updates, criteria):
        self.db_manager.update_records_in_table(table_name, updates, criteria)
        
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

    def convert_object_name_to_management_table_name(self,object_name):
        return f'mng_{object_name}s'
    
    def is_management_table_exist(self, table_name):
        # Check if table exists by querying the sqlite_master table
        query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        return self.db_manager.execute_query_with_single_result(query)
  
    def save_in_memory(self, object):
            # insert object info into management table mng_{object_name}s
            # for exmple: object db_instance will be saved in table mng_db_instances
            table_name = str(object.__class__.__name__)
            if not self.object_manager.is_management_table_exist(table_name):
                self.object_manager.create_management_table(table_name)
            self.object_manager.insert_object_to_management_table(table_name, object.to_sql())
            
    def is_management_table_exist(self, table_name):
            query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            result = self.db_manager.execute_query_with_single_result(query)
            return result is not None
        
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
        
    def convert_object_attributes_to_dictionary(self, **kwargs):
        dict = {}
        for key, value in kwargs.items():
            dict[key] = value
        return dict
    
    def get_all_data_from_table(self, table_name):
        self.db_manager.get_all_data_from_table(table_name)

import sqlite3
from typing import Dict, Any, List, Optional, Tuple
import json
from sqlite3 import OperationalError
class DBManager:
    def __init__(self, db_file: str):
        '''Initialize the database connection and create tables if they do not exist.'''
        self.connection = sqlite3.connect(db_file)
    # saraNoigershel
    def execute_query_with_multiple_results(self, query: str, params:Tuple = ()) -> Optional[List[Tuple]]:
        '''Execute a given query and return the results.'''
        try:
            c = self.connection.cursor()
            c.execute(query, params)
            results = c.fetchall()
            self.connection.commit()
            return results if results else None
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        
    # ShaniStrassProg
    def execute_query_with_single_result(self, query: str, params:Tuple = ()) -> Optional[Tuple]:
        '''Execute a given query and return a single result.'''
        try:
            c = self.connection.cursor()
            c.execute(query, params)
            result = c.fetchone()
            self.connection.commit()
            return result if result else None
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        
    # Riki7649255
    def execute_query_without_results(self, query: str, params:Tuple = ()):
        '''Execute a given query without waiting for any result.'''
        try:
            c = self.connection.cursor()
            c.execute(query, params)
            self.connection.commit()
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        
    # Yael, Riki7649255
    def create_table(self, table_name, table_structure):
        '''create a table in a given db by given table_structure'''
        create_statement = f'''CREATE TABLE IF NOT EXISTS {table_name} ({table_structure})'''
        self.execute_query_without_results(create_statement)
        
    def insert_data_into_table(self, table_name, data):
        insert_statement = f'''INSERT INTO {table_name} VALUES {data}'''
        self.execute_query_without_results(insert_statement)
        
    # Riki7649255 based on rachel-8511, Shani
    def update_records_in_table(self, table_name: str, updates: Dict[str, Any], criteria: Optional[str]) -> None:
        '''Update records in the specified table based on criteria.'''
        # add documentation here
        set_clause = ', '.join([f'{k} = ?' for k in updates.keys()])
        values = tuple(updates.values())
        update_statement = f'''
            UPDATE {table_name}
            SET {set_clause}
        '''
        if criteria:
            update_statement = update_statement + f'''WHERE {criteria}'''
        self.execute_query_without_results(update_statement, values)
        
    # Riki7649255 based on rachel-8511
    def delete_data_from_table(self, table_name: str, criteria: str) -> None:
        '''Delete a record from the specified table based on criteria.'''
        delete_statement = f'''
            DELETE FROM {table_name}
            WHERE {criteria}
        '''
        self.execute_query_without_results(delete_statement)
        
    # Tem-M
    def get_columns_from_table(self, table_name):
        '''Get the columns from the specified table.'''
        try:
            get_columns_query = f"""PRAGMA table_info({table_name});"""
            cols = self.execute_query_with_multiple_results(get_columns_query)
            return [col[1] for col in cols]
        except Exception as e:
            print(f"Error occurred while fetching columns from table {table_name}: {e}")
            return []
        
    def get_all_data_from_table(self, table_name):
        try:
            get_all_data_query = f"""SELECT * FROM {table_name}"""
            return self.execute_query_with_multiple_results(get_all_data_query)
        except Exception as e:
            print(f"Error occurred while fetching data from table {table_name}: {e}")
            return []
        
    # rachel-8511, Riki7649255
    def select_and_return_records_from_table(self, table_name: str, columns: List[str] = ['*'], criteria: Optional[str] = None) -> Dict[int, Dict[str, Any]]:
        '''Select records from the specified table based on criteria.
        Args:
            table_name (str): The name of the table.
            columns (List[str]): The columns to select. Default is all columns ('*').
            criteria (str): SQL condition for filtering records. Default is no filter.
        Returns:
            Dict[int, Dict[str, Any]]: A dictionary where keys are object_ids and values are metadata.
        '''
        cols = columns
        if cols == ['*']:
            cols = self.get_columns_from_table(table_name)
        columns_clause = ', '.join(cols)
        query = f'SELECT {columns_clause} FROM {table_name}'
        if criteria:
            query += f' WHERE {criteria};'
        try:
            results = self.execute_query_with_multiple_results(query)
            return {result[0]: dict(zip(cols if columns != ['*'] else cols[1:], result[1:])) for result in results}
        except OperationalError as e:
            raise Exception(f'Error selecting from {table_name}: {e}')
        except TypeError as e:
            raise Exception(f'Error selecting from {table_name}: {e}')
        
    def is_exists_in_table(self, table_name:str, criteria:str):
        """check if rows exists in table"""
        return self.select_and_return_records_from_table(table_name, criteria=criteria) != {}
    
    # rachel-8511, ShaniStrassProg, Riki7649255
    def describe_table(self, table_name: str) -> Dict[str, str]:
        '''Describe table structure.'''
        try:
            desc_statement = f'PRAGMA table_info({table_name})'
            columns = self.execute_query_with_multiple_results(desc_statement)
            return {col[1]: col[2] for col in columns}
        except OperationalError as e:
            raise Exception(f'Error describing table {table_name}: {e}')
        
    # rachel-8511, ShaniStrassProg
    def close(self):
        '''Close the database connection.'''
        self.connection.close()