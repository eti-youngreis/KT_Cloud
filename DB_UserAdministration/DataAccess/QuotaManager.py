from typing import Dict, Any
import json
import sqlite3
from DB.DataAccess.DBManager import DBManager

class QuotaManager:
    def __init__(self, db_file: str):

        '''Initialize QuotaManager with the database connection.'''
        self.db_manager:DBManager = DBManager(db_file)
        self.table_name = 'quotas'
        self.create_table()
    
    def create_table(self):
        '''create objects table in the database'''
        table_schema = 'quota_id TEXT NOT NULL PRIMARY KEY, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)
        
    def create(self, quota_id: str, metadata: Dict[str, Any]) -> None:
        '''Create a new quota in the database.'''
        self.db_manager.insert(self.table_name, quota_id, metadata)

    def update(self, quota_id: int, metadata: Dict[str, Any]) -> None:
        '''Update an existing object in the database.'''
        self.db_manager.update(self.table_name, {'metadata': json.dumps(metadata)}, f'quota_id = {quota_id}')

    def get(self, quota_id: int) -> Dict[str, Any]:
        '''Retrieve an quota from the database.'''
        result = self.db_manager.select(self.table_name, ['type_object', 'metadata'], f'quota_id = {quota_id}')
        if result:
            return result[quota_id]
        else:
            raise FileNotFoundError(f'Quota with ID {quota_id} not found.')

    def delete(self, quota_id: int) -> None:
        '''Delete an object from the database.'''
        self.db_manager.delete(self.table_name, f'quota_id = {quota_id}')

    def get_all_objects(self) -> Dict[int, Dict[str, Any]]:
        '''Retrieve all objects from the database.'''
        return self.db_manager.select(self.table_name, ['quota_id', 'metadata'])

    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()
