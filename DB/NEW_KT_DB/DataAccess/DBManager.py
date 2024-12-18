import sqlite3
from typing import Dict, Any, List, Optional, Tuple
import json
from sqlite3 import OperationalError

class DBManager:
    def __init__(self, db_file: str):
        '''Initialize the database connection and create tables if they do not exist.'''
        self.db_path = db_file

<<<<<<< HEAD
    def execute_query_with_multiple_results(self, query: str, params:Tuple = ()) -> Optional[List[Tuple]]:
        '''Execute a given query and return the results.'''
        try:
            c = self.connection.cursor()
            c.execute(query, params)
            results = c.fetchall()
            self.connection.commit() 

=======
    def _is_resultset_empty(self, resultset):
        if resultset == None or not resultset:
            return True
        return False

    def execute_query_with_multiple_results(self, query: str):
        '''Execute a given query and return the results.'''
        try:
            connection = sqlite3.connect(self.db_path)
            c = connection.cursor()
            c.execute(query)
            results = c.fetchall()

            if self._is_resultset_empty(results):
                raise EmptyResultsetError('''Error: No records found.
                execute_query_with_multiple_results should return one or more records as a result.
                try to fix your query or use execute_query_without_results() instead.''', query)
            connection.commit()
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
            return results if results else None
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        finally:
            self._close_connection(connection)

<<<<<<< HEAD
    def execute_query_with_single_result(self, query: str, params:Tuple = ()) -> Optional[Tuple]:
        '''Execute a given query and return a single result.'''
        try:
            c = self.connection.cursor()
            c.execute(query, params)
            result = c.fetchone()
            self.connection.commit()
=======
    def execute_query_with_single_result(self, query: str):
        '''Execute a given query and return a single result.'''
        try:
            connection = sqlite3.connect(self.db_path)
            c = connection.cursor()
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

            connection.commit()
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
            return result if result else None
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        finally:
            self._close_connection(connection)


    def execute_query_without_results(self, query: str, params:Tuple = ()):
        '''Execute a given query without waiting for any result.'''
        try:
<<<<<<< HEAD
            c = self.connection.cursor()
            c.execute(query, params)
            self.connection.commit()
=======
            connection = sqlite3.connect(self.db_path)
            c = connection.cursor()
            c.execute(query)
            connection.commit()
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        finally:
            self._close_connection(connection)


<<<<<<< HEAD
    
=======
    def _execute_query_with_or_without_results(self, query: str):
        try:
            connection = sqlite3.connect(self.db_path)
            c = connection.cursor()
            c.execute(query)
            if not c.fetchall():
                optional_results = None
            else:
                optional_results = c.fetchall()
            connection.commit()
            return optional_results
        except OperationalError as e:
            raise Exception(f'Error executing query {query}: {e}')
        finally:
            self._close_connection(connection)


>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
    def create_table(self, table_name, table_structure):
        '''create a table in a given db by given table_structure'''
        create_statement = f'''CREATE TABLE IF NOT EXISTS {table_name} ({table_structure})'''
        self.execute_query_without_results(create_statement)


<<<<<<< HEAD
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
=======
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
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
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
<<<<<<< HEAD
=======
        except EmptyResultsetError as e:
            raise Exception(f"table {table_name} not found: {e}")

>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        except Exception as e:
            raise Exception(f"Error occurred while fetching columns from table {table_name}: {e}")

    def get_all_data_from_table(self, table_name):
        try:
            get_all_data_query = f"""SELECT * FROM {table_name}"""
            return self.execute_query_with_multiple_results(get_all_data_query)
        except Exception as e:
            raise Exception(f"Error occurred while fetching data from table {table_name}: {e}")

    
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
<<<<<<< HEAD
            results = self.execute_query_with_multiple_results(query)
            return {result[0]: dict(zip(cols if columns != ['*'] else cols[1:], result[1:])) for result in results}
        except OperationalError as e:
            raise Exception(f'Error selecting from {table_name}: {e}')
    
    def is_exists_in_table(self, table_name:str, criteria:str):
=======
            if criteria is None:
                get_all_data_query = f"""SELECT {columns} FROM {table_name}"""
            else:
                get_all_data_query = f"""SELECT {columns} FROM {table_name} WHERE {criteria}"""

            return self.execute_query_with_multiple_results(get_all_data_query)
        
        except Exception as e:
            raise Exception(f"Error occurred while fetching data from table {table_name}: {e}")


    def is_object_exist(self, table_name:str, criteria:str):
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        """check if rows exists in table"""
        try:
            in_memory_object = self.get_data_from_table(table_name, criteria=criteria)
            if in_memory_object:
                return True
        except Exception as e:
            raise Exception(f"Error checking if object {criteria} exists in {table_name}: {e}")
        return False


<<<<<<< HEAD
=======
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
            raise Exception(f"Error checking if table {table_name} exists: {e}")
        return False



>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
    def describe_table(self, table_name: str) -> Dict[str, str]:
        '''Describe table structure.'''
        try:
            desc_statement = f'PRAGMA table_info({table_name})'
            columns = self.execute_query_with_multiple_results(desc_statement)
            return {col[1]: col[2] for col in columns}
        except OperationalError as e:
            raise Exception(f'Error describing table {table_name}: {e}')
<<<<<<< HEAD

    def close(self):
        '''Close the database connection.'''
        self.connection.close()
=======
        
        
    def _close_connection(self, connection = None) -> None:
        """Close the database connection if it is open."""
        if connection:
            connection.close()
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
