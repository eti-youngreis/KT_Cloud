from typing import Dict, Any
import json
from DB.DataAccess.DBManager import DBManager

# commit
class PolicyManager:
    def __init__(self, db_file: str):
        '''Initialize PolicyManager with the database connection.'''
        self.db_manager = DBManager(db_file)
        self.table_name = 'policy_management'
        self.create_table()

    def create_table(self):
        '''Create policies table in the database.'''
        table_schema = 'policy_id TEXT NOT NULL PRIMARY KEY, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)

    def create(self, metadata: Dict[str, Any]) -> None:
        '''Create a new policy in the database.'''
        self.db_manager.insert(self.table_name, metadata)

    def is_json_column_contains_key_and_value(self, key: str, value: str) -> bool:
        '''Check if the JSON column contains the specified key and value.'''
        return self.db_manager.is_json_column_contains_key_and_value(self.table_name, key, value)

    def is_identifier_exist(self, value: str) -> bool:
        '''Check if a policy with the specified ID exists in the database.'''
        return self.db_manager.is_identifier_exist(self.table_name, value)

    def update(self, policy_id: str, metadata: Dict[str, Any]) -> None:
        '''Update an existing policy in the database.'''
        self.db_manager.update(self.table_name, {'metadata': json.dumps(metadata)}, f'policy_id = {policy_id}')

    def get(self, policy_id: str) -> Dict[str, Any]:
        '''Retrieve a policy from the database.'''
        result = self.db_manager.select(self.table_name, ['metadata'], f'policy_id = {policy_id}')
        if result:
            return result[policy_id]
        else:
            raise FileNotFoundError(f'Policy with ID {policy_id} not found.')

    def delete(self, policy_id: int) -> None:
        '''Delete a policy from the database.'''
        self.db_manager.delete(self.table_name, f'policy_id = {policy_id}')

    def get_all_policies(self) -> Dict[int, Any]:
        '''Retrieve all policies from the database.'''
        return self.db_manager.select(self.table_name, ['policy_id', 'metadata'])

    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()
