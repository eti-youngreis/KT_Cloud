from typing import Dict, Any
import json
import sqlite3
from DBManager import DBManager

class UserManager:

    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.db_manager = DBManager(db_file)
        self.table_name ='users'
        self.create_table()
    
    def create_table(self):
        '''create objects table in the database'''
        table_schema = 'user_id TEXT NOT NULL PRIMARY KEY AUTOINCREMENT, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)
  
    def create(self, metadata: Dict[str, Any]) -> None:
        '''Create a new object in the database.'''
        self.db_manager.insert(self.table_name, metadata)

    def is_json_column_contains_key_and_value(self, key: str, value: str) -> bool:
        self.db_manager.is_json_column_contains_key_and_value(self.table_name, key, value)

    def is_identifier_exit(self, value: str):
        self.db_manager.is_identifier_exit(self.table_name, value)
    
    def is_value_exit_in_column(self, table_name: str, column: str, value: str):
        self.db_manager.is_value_exit_in_column( table_name, column, value)

    def update(self, user_id: str, user_name: str) -> None:
        '''Update an existing object in the database.'''
        self.db_manager.update(self.table_name, {'metadata': json.dumps({'user_name': user_name})}, f'user_id = {user_id}')


    def get(self, object_id: int) -> Dict[str, Any]:
        '''Retrieve an object from the database.'''
        result = self.db_manager.select(self.table_name, ['metadata'], f'object_id = {object_id}')
        if result:
            return result[object_id]
        else:
            raise FileNotFoundError(f'Object with ID {object_id} not found.')


    def delete(self, user_id: str) -> None:
        '''Delete an object from the database.'''
        self.db_manager.delete(self.table_name, f'user_id = {user_id}')

    def get_all_objects(self) -> Dict[int, Any]:
        '''Retrieve all objects from the database.'''
        return self.db_manager.select(self.table_name, ['object_id', 'metadata'])
    

    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)
    

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()

