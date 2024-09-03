from typing import Dict, Any, Optional
import json

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DB_UserAdministration.Models.PolicyModel import Policy


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from DB_UserAdministration.DataAccess.DBManager import DBManager

# commit
class PolicyManager(DBManager):
    def __init__(self, db_file: str):
        '''Initialize PolicyManager with the database connection.'''
        self.table_name = 'policy_management'
        self.identifier_param = 'policy_id'
        self.columns = [self.identifier_param, 'permissions']
        super().__init__(db_file=db_file)

    def create_table(self):
        '''Create policies table in the database.'''
        table_schema = f'{self.identifier_param} TEXT NOT NULL PRIMARY KEY, permissions TEXT'
        super().create_table(self.table_name, table_schema)

    def insert(self, metadata: Dict[str, Any], object_id:Optional[Any] = None) -> None:
        '''Create a new policy in the database.'''
        super().insert(metadata, object_id)

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
        
    def list_policies(self) -> Dict[str, Policy]:
        result = self.select()
        print(result)
        return list(Policy.build_from_dict(policy) for policy in result)
    
    def delete(self, policy_id: int) -> None:
        '''Delete a policy from the database.'''
        self.db_manager.delete(self.table_name, f'policy_id = {policy_id}')

    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()
