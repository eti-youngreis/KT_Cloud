from typing import Dict, Any
import json
from DB.DataAccess.DBManager import DBManager

class QuotaManager:
    def __init__(self, db_file: str):

        '''Initialize QuotaManager with the database connection.'''
        self.db_manager:DBManager = DBManager(db_file)
        self.table_name = 'quotas'
        self.create_table()
    
    def create_table(self):
        '''create objects table in the database'''
        table_schema = 'id TEXT NOT NULL PRIMARY KEY, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)
        
    def create(self, quota_id: str, metadata: Dict[str, Any]) -> None:
        '''Create a new quota in the database.'''
        self.db_manager.insert(self.table_name, metadata, quota_id)

    def update(self, quota_id: str, metadata: Dict[str, Any]) -> None:
        '''Update an existing object in the database.'''
        self.db_manager.update(self.table_name, {'metadata': json.dumps(metadata)}, f'id = "{quota_id}"')

    def get(self, quota_id: str) -> Dict[str, Any]:
        '''Retrieve an quota from the database.'''
        result = self.db_manager.select(self.table_name, ['quota_id','metadata'], f'id = {quota_id}')
        if result:
            return result[quota_id]
        else:
            raise FileNotFoundError(f'Quota with ID {quota_id} not found.')

    def delete(self, quota_id: str) -> None:
        '''Delete an object from the database.'''
        self.db_manager.delete(self.table_name, f'id = {quota_id}')

    def get_all_quotas(self) -> Dict[str, Dict[str, Any]]:
        '''Retrieve all objects from the database.'''
        return self.db_manager.select(self.table_name, ['metadata'])
    
    def get_quotas_by_owner_id(self, owner_id:str):
        # Using SQL JSON functions to query based on a JSON column
        criteria = f"JSON_EXTRACT(metadata, '$.owner_id') = '{owner_id}'"
        # Execute the query with the given owner_id as parameter
        return self.db_manager.select(self.table_name, ['metadata'], criteria)


    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()
