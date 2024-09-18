import sqlite3
from typing import Dict, Any, List, Optional, Tuple
import json
from sqlite3 import OperationalError

class EmptyResultsetError(Exception):
    def __init__(self, message, query):
        super().__init__(message)
        self.query = query

    def __str__(self):
        return f"{self.args[0]} | Query: {self.query}"

class DBManager:
    def __init__(self, db_file: str):
        '''Initialize the database connection and create tables if they do not exist.'''
        self.connection = sqlite3.connect(db_file)


    def _is_resultset_empty(self, resultset):
        if resultset == None or not resultset:
            return True
        return False

    def execute_query_with_multiple_results(self, query: str):
        '''Execute a given query and return the results.'''
        try:
            c = self.connection.cursor()
            c.execute(query)
            results = c.fetchall()

            if self._is_resultset_empty(results):
                raise EmptyResultsetError('''Error: No records found.
                execute_query_with_multiple_results should return one or more records as a result.
                try to fix your query or use execute_query_without_results() instead.''', query)

            self.connection.commit() 
            return results if results else None
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')


    def execute_query_with_single_result(self, query: str):
        '''Execute a given query and return a single result.'''
        try:
            c = self.connection.cursor()
            c.execute(query)
            result = c.fetchone()
            # Check if more than one result exists
            extra_result = c.fetchone()

            if result is None:
                raise EmptyResultsetError('''Error: No records found.
                execute_query_with_single_result should return exactly one record as a result.
                try to fix your query or use execute_query_without_results() instead.''', query)

            if extra_result is not None:
                raise Exception('''Error: more than one record found.
                execute_query_with_single_result should return exactly one record as a result.
                try to fix your query or use execute_query_with_multiple_results() instead.''')

            self.connection.commit()
            return result if result else None
        
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')


    def execute_query_without_results(self, query: str):
        '''Execute a given query without waiting for any result.'''
        try:
            c = self.connection.cursor()
            c.execute(query)
            self.connection.commit()
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')


    def _execute_query_with_or_without_results(self, query: str):
        try:
            c = self.connection.cursor()
            c.execute(query)
            if not c.fetchall():
                optional_results = None
            else:
                optional_results = c.fetchall()
            self.connection.commit()
            return optional_results
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')


    def create_table(self, table_name, table_structure):
        '''create a table in a given db by given table_structure'''
        create_statement = f'''CREATE TABLE IF NOT EXISTS {table_name} ({table_structure})'''
        self.execute_query_without_results(create_statement)


    def insert_data_into_table(self, table_name, data, columns=None):
        if columns is None:
            insert_statement = f'''INSERT INTO {table_name} VALUES {data}'''
        else:
            insert_statement = f'''INSERT INTO {table_name}({columns}) VALUES {data}'''
        self.execute_query_without_results(insert_statement)


    def update_records_in_table(self, table_name: str, updates: str, criteria=None) -> None:
        '''Update records in the specified table based on criteria.'''

        update_statement = f'''
            UPDATE {table_name}
            SET {updates}
        '''
        if criteria:
            update_statement = update_statement + f''' WHERE {criteria}'''

        self.execute_query_without_results(update_statement)


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
            print(cols)
            return [col[1] for col in cols]
        except EmptyResultsetError as e:
            print(f"table {table_name} not found: {e}")
            return []

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


    def get_data_from_table(self, table_name, columns='*', criteria=None):
        try:
            if criteria is None:
                get_all_data_query = f"""SELECT {columns} FROM {table_name}"""
            else:
                get_all_data_query = f"""SELECT {columns} FROM {table_name} WHERE {criteria}"""

            return self.execute_query_with_multiple_results(get_all_data_query)
        
        except Exception as e:
            print(f"Error occurred while fetching data from table {table_name}: {e}")
            return []


    def is_object_exist(self, table_name:str, criteria:str):
        """check if rows exists in table"""
        try:
            in_memory_object = self.get_data_from_table(table_name, criteria=criteria)
            if in_memory_object:
                return True
        except Exception as e:
            print(Exception(f"Error checking if object {criteria} exists in {table_name}: {e}"))
            return False


    def is_table_exist(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = f'''
            SELECT name 
            FROM sqlite_master 
            WHERE type='table' AND name='{table_name}'
        '''
        try:
            result = self.execute_query_with_single_result(query)
            return True
        except Exception as e:
            print(Exception(f"Error checking if table {table_name} exists: {e}"))
            return False



    def describe_table(self, table_name: str) -> Dict[str, str]:
        '''Describe table structure.'''
        try:
            if self.is_table_exist(table_name):
                desc_statement = f'PRAGMA table_info({table_name})'
                columns = self.execute_query_with_multiple_results(desc_statement)
                return {col[1]: col[2] for col in columns}
            else:
                raise Exception(f'table {table_name} not found')
        except OperationalError as e:
            raise Exception(f'Error describing table {table_name}: {e}')
