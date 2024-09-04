from typing import Dict, Any
import json
import sqlite3
from DB.DataAccess.DBManager import DBManager
class UserManager:

    def __init__(self, db_file: str):
        '''Initialize UserManager with the database connection.'''
        self.db_manager = DBManager(db_file)
        self.table_name ='users'
        self.create_table()
    
    def create_table(self):
        '''create users table in the database'''
        table_schema = 'user_id TEXT NOT NULL PRIMARY KEY, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)
  
    def create(self, metadata: Dict[str, Any]) -> None:
        '''Create a new user in the database.'''
        self.db_manager.insert(self.table_name, metadata)

    def is_json_column_contains_key_and_value(self, key: str, value: str) -> bool:
        self.db_manager.is_json_column_contains_key_and_value(self.table_name, key, value)

    def is_identifier_exist(self, value: str):
        self.db_manager.is_identifier_exist(self.table_name, value)
    
    def is_value_exist_in_column(self, table_name: str, column: str, value: str):
        self.db_manager.is_value_exit_in_column( table_name, column, value)

    def update(self, user_id: str, user_name: str) -> None:
        '''Update an existing user in the database.'''
        self.db_manager.update(self.table_name, {'metadata': json.dumps({'user_name': user_name})}, f'user_id = {user_id}')

    def delete(self, user_id: str) -> None:
        '''Delete an user from the database.'''
        self.db_manager.delete(self.table_name, f'user_id = {user_id}')

    def get(self, user_id: int) -> Dict[str, Any]:
        '''Retrieve an user from the database.'''
        result = self.db_manager.select(self.table_name, ['metadata'], f'user_id = {user_id}')
        if result:
            return result['metadata']
        else:
            raise FileNotFoundError(f'User with ID {user_id} not found.')

    def get_all_users(self) -> Dict[int, Any]:
        '''Retrieve all users from the database.'''
        return self.db_manager.select(self.table_name, ['user_id', 'metadata'])
    

    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)
    

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()
