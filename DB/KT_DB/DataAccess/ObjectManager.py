from typing import Dict, Any
import json
import sqlite3
from DBManager import DBManager

class ObjectManager:
    def __init__(self, db_file: str):

        '''Initialize ObjectManager with the database connection.'''
        self.db_manager = DBManager(db_file)
        self.table_name = 'objects'
        self.create_table()
    
    def create_table(self):
        '''create objects table in the database'''
        table_schema = 'object_id INTEGER PRIMARY KEY AUTOINCREMENT,type_object TEXT NOT NULL,metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)
        
    def create(self, object_type: str, metadata: Dict[str, Any]) -> None:
        '''Create a new object in the database.'''
        self.db_manager.insert(self.table_name, object_type, metadata)

    def update(self, object_id: int, metadata: Dict[str, Any]) -> None:
        '''Update an existing object in the database.'''
        self.db_manager.update(self.table_name, {'metadata': json.dumps(metadata)}, f'object_id = {object_id}')

    def get(self, object_id: int) -> Dict[str, Any]:
        '''Retrieve an object from the database.'''
        result = self.db_manager.select(self.table_name, ['type_object', 'metadata'], f'object_id = {object_id}')
        if result:
            return result[object_id]
        else:
            raise FileNotFoundError(f'Object with ID {object_id} not found.')

    def delete(self, object_id: int) -> None:
        '''Delete an object from the database.'''
        self.db_manager.delete(self.table_name, f'object_id = {object_id}')

    def get_all_objects(self) -> Dict[int, Dict[str, Any]]:
        '''Retrieve all objects from the database.'''
        return self.db_manager.select(self.table_name, ['object_id', 'type_object', 'metadata'])

    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()
