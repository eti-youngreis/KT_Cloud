import sqlite3
from typing import Dict, Any, List, Optional, Tuple
import json
from sqlite3 import OperationalError

class DBManager:
    def __init__(self, db_file: str):
        '''Initialize the database connection and create tables if they do not exist.'''
        self.connection = sqlite3.connect(db_file)
    
    def create_table(self, table_name, table_schema):
        '''create a table in a given db by given table_schema'''
        with self.connection:
            self.connection.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})
            ''')

    def insert(self, table_name: str, metadata: Dict[str, Any], object_id: Optional[Any] = None) -> None:
        '''Insert a new record into the specified table.'''
        metadata_json = json.dumps(metadata)
        try:
            c = self.connection.cursor()
            if object_id is not None:
                # Assuming the ID column is named 'id' and it's the first column
                c.execute(f'''
                    INSERT INTO {table_name} (id, metadata)
                    VALUES (?, ?)
                ''', (object_id, metadata_json))
            else:
                # Insert without the ID, assuming the ID is auto-increment
                c.execute(f'''
                    INSERT INTO {table_name} (metadata)
                    VALUES (?)
                ''', (metadata_json,))
            self.connection.commit()
        except OperationalError as e:
            raise Exception(f'Error inserting into {table_name}: {e}')



    def update(self, table_name: str, updates: Dict[str, Any], criteria: str) -> None:
        '''Update records in the specified table based on criteria.'''
        set_clause = ', '.join([f'{k} = ?' for k in updates.keys()])
        values = list(updates.values())
        
        query = f'''
            UPDATE {table_name}
            SET {set_clause}
            WHERE {criteria}
        '''
        try:
            c = self.connection.cursor()
            c.execute(query, values)
            self.connection.commit()
        except OperationalError as e:
            raise Exception(f'Error updating {table_name}: {e}')


    def select(self, table_name: str, columns: List[str] = ['*'], criteria: str = '') -> Dict[int, Dict[str, Any]]:
        '''Select records from the specified table based on criteria.
        Args:
            table_name (str): The name of the table.
            columns (List[str]): The columns to select. Default is all columns ('*').
            criteria (str): SQL condition for filtering records. Default is no filter.
        Returns:
            Dict[int, Dict[str, Any]]: A dictionary where keys are object_ids and values are metadata.
        '''
        columns_clause = ', '.join(columns)
        query = f'SELECT {columns_clause} FROM {table_name}'
        if criteria:
            query += f' WHERE {criteria}'
        try:
            c = self.connection.cursor()
            c.execute(query)
            results = c.fetchall()
            print(results)
            return {result[0]: dict(zip(columns, result[1:])) for result in results}
        except OperationalError as e:
            raise Exception(f'Error selecting from {table_name}: {e}')
        
    def delete(self, table_name: str, criteria: str) -> None:
        '''Delete a record from the specified table based on criteria.'''
        try:
            c = self.connection.cursor()
            c.execute(f'''
                DELETE FROM {table_name}
                WHERE {criteria}
            ''')
            self.connection.commit()
        except OperationalError as e:
            raise Exception(f'Error deleting from {table_name}: {e}')

    def describe(self, table_name: str) -> Dict[str, str]:
        '''Describe the schema of the specified table.'''
        try:
            c = self.connection.cursor()
            c.execute(f'PRAGMA table_info({table_name})')
            columns = c.fetchall()
            return {col[1]: col[2] for col in columns}
        except OperationalError as e:
            raise Exception(f'Error describing table {table_name}: {e}')
    
    

    def execute_query(self, query: str) -> Optional[List[Tuple]]:
        '''Execute a given query and return the results.'''
        try:
            c = self.connection.cursor()
            c.execute(query)
            results = c.fetchall()
            return results if results else None
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')

    def execute_query_with_single_result(self, query: str) -> Optional[Tuple]:
        '''Execute a given query and return a single result.'''
        try:
            c = self.connection.cursor()
            c.execute(query)
            result = c.fetchone()
            return result if result else None
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        
    def is_json_column_contains_key_and_value(self, table_name: str, key: str, value: str) -> bool:
        '''Check if a specific key-value pair exists within a JSON column in the given table.'''
        try:
            c = self.connection.cursor()
            # Properly format the LIKE clause with escaped quotes for key and value
            c.execute(f'''
            SELECT COUNT(*) FROM {table_name}
            WHERE metadata LIKE ?
            LIMIT 1
            ''', (f'%"{key}": "{value}"%',))
            # Check if the count is greater than 0, indicating the key-value pair exists
            return c.fetchone()[0] > 0
        except sqlite3.OperationalError as e:
            print(f'Error: {e}')
            return False

    def is_identifier_exist(self, table_name: str, value: str) -> bool:
        '''Check if a specific value exists within a column in the given table.'''
        try:
            c = self.connection.cursor()
            c.execute(f'''
            SELECT COUNT(*) FROM {table_name}
            WHERE id LIKE ?
            ''', (value,))
            return c.fetchone()[0] > 0
        except sqlite3.OperationalError as e:
            print(f'Error: {e}')
    
    def close(self):
        '''Close the database connection.'''
        self.connection.close()


    def update_metadata(self, table_name: str, user_id: int, key: str, value: Any, action: str = 'set') -> None:
        """
        Generic function to update a specific part of the metadata for a given user.
        
        Parameters:
        - table_name: The name of the table containing the metadata.
        - user_id: The ID of the user whose metadata needs to be updated.
        - key: The specific part of the metadata to update (e.g., 'password', 'roles', 'policies', 'quotas').
        - value: The value to update, append, or delete (could be a new policy, role, password, etc.).
        - action: The type of update to perform ('set' for replacing, 'append' for adding to lists, 'update' for dicts, 'delete' for removing).
        """
        
        # Step 1: Retrieve the current metadata for the user
        query = f"SELECT metadata FROM {table_name} WHERE user_id = ?"
        try:
            c = self.connection.cursor()
            c.execute(query, (user_id,))
            row = c.fetchone()
            if row:
                metadata = json.loads(row[0])  # Assuming metadata is stored as a JSON string
            else:
                raise Exception(f"User with id {user_id} not found.")
        except OperationalError as e:
            raise Exception(f'Error fetching metadata: {e}')
        
        # Step 2: Modify the relevant part of the metadata
        if key not in metadata:
            raise KeyError(f"Key '{key}' not found in metadata.")
        
        if action == 'set':
            # Replace the value of the key directly
            metadata[key] = value
        elif action == 'append' and isinstance(metadata[key], list):
            # Append to the list (for roles, policies, etc.)
            metadata[key].append(value)
        elif action == 'update' and isinstance(metadata[key], dict):
            # Update a dictionary (for quotas or nested data)
            metadata[key].update(value)
        elif action == 'delete':
            if isinstance(metadata[key], list):
                # Remove an item from a list
                if value in metadata[key]:
                    metadata[key].remove(value)
                else:
                    raise ValueError(f"Value '{value}' not found in list '{key}'.")
            elif isinstance(metadata[key], dict):
                # Remove a key from a dictionary
                if value in metadata[key]:
                    del metadata[key][value]
                else:
                    raise ValueError(f"Key '{value}' not found in dictionary '{key}'.")
            else:
                raise ValueError(f"Action 'delete' is not supported for the data type of key '{key}'.")
        else:
            raise ValueError(f"Invalid action '{action}' or incompatible data type for key '{key}'.")
        
        # Step 3: Serialize metadata back to JSON string
        updated_metadata = json.dumps(metadata)
        
        # Step 4: Use your update function to write the new metadata back to the database
        updates = {"metadata": updated_metadata}
        criteria = f"user_id = {user_id}"
        self.update(table_name, updates, criteria)


        