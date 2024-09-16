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

    # Riki7649255  based on rachel-8511, ShaniStrassProg 
    def insert_data_into_table(self, table_name, columns, data):
        column_names = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        self.execute_query_without_results(insert_query, data)


    # Riki7649255 based on rachel-8511, Shani
    def update_records_in_table(self, table_name: str, updates: Dict[str, Any], criteria: Optional[str]) -> None:
        '''Update records in the specified table based on criteria.'''

        # add documentation here
        set_clause = ', '.join([f'{k} = ?' for k in updates.keys()])
        values = list(updates.values())
    
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
        
        if columns == ['*']:
            columns = [res[1] for res in self.connection.execute(f'PRAGMA table_info({table_name});').fetchall()]
            
        columns_clause = ', '.join(columns)
        query = f'SELECT {columns_clause} FROM {table_name}'
        if criteria:
            query += f' WHERE {criteria};'
        
        try:
            results = self.execute_query_with_multiple_results(query)
            return {result[0]: dict(zip(columns, result[1:])) for result in results}
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
    
    
    # ShaniStrassProg
    # should be in ObjectManager and send the query to one of the execute_query functions
    # def is_json_column_contains_key_and_value(self, table_name: str, key: str, value: str) -> bool:
    #     '''Check if a specific key-value pair exists within a JSON column in the given table.'''
    #     try:
    #         c = self.connection.cursor()
    #         # Properly format the LIKE clause with escaped quotes for key and value
    #         c.execute(f'''
    #         SELECT COUNT(*) FROM {table_name}
    #         WHERE metadata LIKE ?
    #         LIMIT 1
    #         ''', (f'%"{key}": "{value}"%',))
    #         # Check if the count is greater than 0, indicating the key-value pair exists
    #         return c.fetchone()[0] > 0
    #     except sqlite3.OperationalError as e:
    #         print(f'Error: {e}')
    #         return False


    # Yael, ShaniStrassProg
    # should be in ObjectManager and send the query to one of the execute_query functions
    # def is_identifier_exist(self, table_name: str, value: str) -> bool:
    #     '''Check if a specific value exists within a column in the given table.'''
    #     try:
    #         c = self.connection.cursor()
    #         c.execute(f'''
    #         SELECT COUNT(*) FROM {table_name}
    #         WHERE id LIKE ?
    #         ''', (value,))
    #         return c.fetchone()[0] > 0
    #     except sqlite3.OperationalError as e:
    #         print(f'Error: {e}')
    

    # sara-lea
    # should be in ObjectManager and send the query to one of the execute_query functions
    # def update_metadata(self, table_name: str, user_id: int, key: str, value: Any, action: str = 'set') -> None:
    #     """
    #     Generic function to update a specific part of the metadata for a given user.
        
    #     Parameters:
    #     - table_name: The name of the table containing the metadata.
    #     - user_id: The ID of the user whose metadata needs to be updated.
    #     - key: The specific part of the metadata to update (e.g., 'password', 'roles', 'policies', 'quotas').
    #     - value: The value to update, append, or delete (could be a new policy, role, password, etc.).
    #     - action: The type of update to perform ('set' for replacing, 'append' for adding to lists, 'update' for dicts, 'delete' for removing).
    #     """
        
    #     # Step 1: Retrieve the current metadata for the user
    #     query = f"SELECT metadata FROM {table_name} WHERE user_id = ?"
    #     try:
    #         c = self.connection.cursor()
    #         c.execute(query, (user_id,))
    #         row = c.fetchone()
    #         if row:
    #             metadata = json.loads(row[0])  # Assuming metadata is stored as a JSON string
    #         else:
    #             raise Exception(f"User with id {user_id} not found.")
    #     except OperationalError as e:
    #         raise Exception(f'Error fetching metadata: {e}')
        
    #     # Step 2: Modify the relevant part of the metadata
    #     if key not in metadata:
    #         raise KeyError(f"Key '{key}' not found in metadata.")
        
    #     if action == 'set':
    #         # Replace the value of the key directly
    #         metadata[key] = value
    #     elif action == 'append' and isinstance(metadata[key], list):
    #         # Append to the list (for roles, policies, etc.)
    #         metadata[key].append(value)
    #     elif action == 'update' and isinstance(metadata[key], dict):
    #         # Update a dictionary (for quotas or nested data)
    #         metadata[key].update(value)
    #     elif action == 'delete':
    #         if isinstance(metadata[key], list):
    #             # Remove an item from a list
    #             if value in metadata[key]:
    #                 metadata[key].remove(value)
    #             else:
    #                 raise ValueError(f"Value '{value}' not found in list '{key}'.")
    #         elif isinstance(metadata[key], dict):
    #             # Remove a key from a dictionary
    #             if value in metadata[key]:
    #                 del metadata[key][value]
    #             else:
    #                 raise ValueError(f"Key '{value}' not found in dictionary '{key}'.")
    #         else:
    #             raise ValueError(f"Action 'delete' is not supported for the data type of key '{key}'.")
    #     else:
    #         raise ValueError(f"Invalid action '{action}' or incompatible data type for key '{key}'.")
        
    #     # Step 3: Serialize metadata back to JSON string
    #     updated_metadata = json.dumps(metadata)
        
    #     # Step 4: Use your update function to write the new metadata back to the database
    #     updates = {"metadata": updated_metadata}
    #     criteria = f"user_id = {user_id}"
    #     self.update(table_name, updates, criteria)