import sqlite3
from typing import Dict, Any, List, Optional, Tuple
import json
from sqlite3 import OperationalError

class DBManager:
    def __init__(self, db_file: str):
        '''Initialize the database connection and create tables if they do not exist.'''
        self.connection = sqlite3.connect(db_file)

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


    def execute_query_without_results(self, query: str, params:Tuple = ()):
        '''Execute a given query without waiting for any result.'''
        try:
            c = self.connection.cursor()
            c.execute(query, params)
            self.connection.commit()
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')


    
    def create_table(self, table_name, table_structure):
        '''create a table in a given db by given table_structure'''
        create_statement = f'''CREATE TABLE IF NOT EXISTS {table_name} ({table_structure})'''
        self.execute_query_without_results(create_statement)


    def insert_data_into_table(self, table_name, data):
        insert_statement = f'''INSERT INTO {table_name} VALUES {data}'''
        self.execute_query_without_results(insert_statement)


    def update_records_in_table(self, table_name: str, updates: Dict[str, Any], criteria: Optional[str]) -> None:
        '''Update records in the specified table based on criteria.'''

        # add documentation here
        set_clause = ', '.join([f'{k} = ?' for k in updates.keys()])
        values = tuple(updates.values())

        update_statement = f'''
            UPDATE {table_name}
            SET {set_clause}
        '''
        if criteria:
            update_statement = update_statement + f'''WHERE {criteria}'''

        self.execute_query_without_results(update_statement, values)


    def delete_data_from_table(self, table_name: str, criteria: str) -> None:
        '''Delete a record from the specified table based on criteria.'''

        delete_statement = f'''
            DELETE FROM {table_name}
            WHERE {criteria}
        '''

        self.execute_query_without_results(delete_statement)
    
    def get_column_names_of_table(self, table_name):
        '''Get the columns from the specified table.'''
        try:
            get_columns_query = f"""PRAGMA table_info({table_name});"""
            cols = self.execute_query_with_multiple_results(get_columns_query)
            return [col[1] for col in cols]
        except Exception as e:
            print(f"Error occurred while fetching columns from table {table_name}: {e}")
            return []

    def get_all_data_from_table(self, table_name):
        try:
            get_all_data_query = f"""SELECT * FROM {table_name}"""
            return self.execute_query_with_multiple_results(get_all_data_query)
        except Exception as e:
            print(f"Error occurred while fetching data from table {table_name}: {e}")
            return []

    
    def select_and_return_records_from_table(self, table_name: str, columns: List[str] = ['*'], criteria: Optional[str] = None) -> Dict[int, Dict[str, Any]]:
        '''Select records from the specified table based on criteria.
        Args:
            table_name (str): The name of the table.
            columns (List[str]): The columns to select. Default is all columns ('*').
            criteria (str): SQL condition for filtering records. Default is no filter.
        Returns:
            Dict[int, Dict[str, Any]]: A dictionary where keys are object_ids and values are metadata.
        '''
        cols = columns
        if cols == ['*']:
            cols = self.get_column_names_of_table(table_name)

        columns_clause = ', '.join(cols)
        query = f'SELECT {columns_clause} FROM {table_name}'
        if criteria:
            query += f' WHERE {criteria};'
        try:
            results = self.execute_query_with_multiple_results(query)
            return {result[0]: dict(zip(cols if columns != ['*'] else cols[1:], result[1:])) for result in results}
        except OperationalError as e:
            raise Exception(f'Error selecting from {table_name}: {e}')
        except TypeError as e:
            raise Exception(f'Error selecting from {table_name}: {e}')

    def is_object_exist(self, table_name:str, criteria:str):
        """check if rows exists in table"""
        return self.select_and_return_records_from_table(table_name, criteria=criteria) != {}


    def describe_table(self, table_name: str) -> Dict[str, str]:
        '''Describe table structure.'''
        try:
            desc_statement = f'PRAGMA table_info({table_name})'
            columns = self.execute_query_with_multiple_results(desc_statement)
            return {col[1]: col[2] for col in columns}
        except OperationalError as e:
            raise Exception(f'Error describing table {table_name}: {e}')

    def close(self):
        '''Close the database connection.'''
        self.connection.close()
