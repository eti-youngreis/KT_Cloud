from typing import Dict, Any,Optional
import json
from DB.DataAccess.DBManager import DBManager


class UserGroupManager:
    def __init__(self, db_file: str):
        '''Initialize UserGroupManager with the database connection.'''
        self.db_manager = DBManager(db_file)
        self.table_name = 'UserGroups'
        self.create_table()

    def create_table(self):
        '''Create groups table in the database.'''
        table_schema = 'id TEXT NOT NULL PRIMARY KEY, metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)

    def create(self, metadata: Dict[str, Any],object_id: Optional[Any]=None) -> None:
        '''Create a new group in the database.'''
        self.db_manager.insert(self.table_name, metadata,object_id)

    def is_json_column_contains_key_and_value(self, key: str, value: str) -> bool:
        '''Check if the JSON column contains the specified key and value.'''
        return self.db_manager.is_json_column_contains_key_and_value(self.table_name, key, value)

    def is_identifier_exist(self, value: str) -> bool:
        '''Check if a group with the specified ID exists in the database.'''
        return self.db_manager.is_identifier_exist(self.table_name, value)

    def update(self, group_id: str, metadata: Dict[str, Any]) -> None:
        '''Update an existing group in the database.'''
        self.db_manager.update(self.table_name, {'metadata': json.dumps(metadata),'id':metadata.get('name')}, f"id = '{group_id}'")

    def get(self, group_id: str) -> Dict[str, Any]:
        '''Retrieve a group from the database.'''
        result = self.db_manager.select(self.table_name, columns=['id','metadata'], criteria= f"id = '{group_id}'")
        if result:
            return json.loads(result[group_id].get('id'))
        else:
            raise FileNotFoundError(f'Group with ID {group_id} not found.')

    def delete(self, group_id: str) -> None:
        '''Delete a group from the database.'''
        self.db_manager.delete(self.table_name, f"id = '{group_id}'")

    def get_all_groups(self) -> Dict[int, Any]:
        '''Retrieve all groups from the database.'''
        return self.db_manager.select(self.table_name, ['id', 'metadata'])

    def describe_table(self) -> Dict[str, str]:
        '''Describe the schema of the table.'''
        return self.db_manager.describe(self.table_name)

    def close(self):
        '''Close the database connection.'''
        self.db_manager.close()
