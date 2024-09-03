import sqlite3
from typing import Dict, Any, List, Tuple, Optional
import json
from sqlite3 import OperationalError


class DBManager:
    def __init__(self, db_file: str):
        """Initialize the database connection and create tables if they do not exist."""
        self.connection = sqlite3.connect(db_file)
        self.create_table()

    def create_table(self, table_name, table_schema):
        """create a table in a given db by given table_schema"""
        table_schema += ", MetaData TEXT"
        with self.connection:
            self.connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})
            """
            )

    def insert(self, metadata: Dict[str, Any], object_id: Optional[Any] = None
    ) -> None:
        """Insert a new record into the specified table."""
        columns = ", ".join(metadata.keys())
        placeholders = ", ".join(["?" for _ in metadata])
        values = list(metadata.values())

        if object_id is not None:
            columns = self.identifier_param + ", " + columns
            placeholders = "?, " + placeholders
            values.insert(0, object_id)

        values = [str(value) for value in values]
        try:
            c = self.connection.cursor()
            c.execute(
                f"""
                INSERT INTO {self.table_name} ({columns})
                VALUES ({placeholders})
                """,
                values,
            )
            self.connection.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error inserting into {self.table_name}: {e}")

    def update(self, table_name: str, updates: Dict[str, Any], criteria: str) -> None:
        """Update records in the specified table based on criteria."""
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())

        query = f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE {criteria}
        """
        try:
            c = self.connection.cursor()
            c.execute(query, values)
            self.connection.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error updating {table_name}: {e}")

    def select(
        self, columns: List[str] = ["*"], criteria: str = ""
    ) -> Dict[int, Dict[str, Any]]:
        """Select records from the specified table based on criteria.

        Args:
            columns (List[str]): The columns to select. Default is all columns ('*').
            criteria (str): SQL condition for filtering records. Default is no filter.

        Returns:
            Dict[int, Dict[str, Any]]: A dictionary where keys are object_ids and values are metadata.
        """
        columns_list = [self.identifier_param].extend(columns) if columns != ['*'] else self.columns
        columns_clause = ", ".join(columns_list)
        query = f"SELECT {columns_clause} FROM {self.table_name}"
        if criteria:
            query += f" WHERE {criteria}"

        try:
            c = self.connection.cursor()
            c.execute(query)
            results = c.fetchall()
            return [dict(zip(columns_list, result)) for result in results]
        except sqlite3.OperationalError as e:
            raise Exception(f"Error selecting from {self.table_name}: {e}")

    def delete(self, criteria: str) -> None:
        """Delete a record from the specified table based on criteria."""
        try:
            c = self.connection.cursor()
            c.execute(
                f"""
                DELETE FROM {self.table_name}
                WHERE {criteria}
            """
            )
            self.connection.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error deleting from {self.table_name}: {e}")

    def describe(self) -> Dict[str, str]:
        """Describe the schema of the specified table."""
        try:
            c = self.connection.cursor()
            c.execute(f"PRAGMA table_info({self.table_name})")
            columns = c.fetchall()
            return {col[1]: col[2] for col in columns}
        except sqlite3.OperationalError as e:
            raise Exception(f"Error describing table {self.table_name}: {e}")

    def execute_query(self, query: str) -> Optional[List[Tuple]]:
        """Execute a given query and return the results."""
        try:
            c = self.connection.cursor()
            c.execute(query)
            results = c.fetchall()
            return results if results else None
        except sqlite3.OperationalError as e:
            raise Exception(f"Error executing query {query}: {e}")

    def execute_query_with_single_result(self, query: str) -> Optional[Tuple]:
        """Execute a given query and return a single result."""
        try:
            c = self.connection.cursor()
            c.execute(query)
            result = c.fetchone()
            return result if result else None
        except sqlite3.OperationalError as e:
            raise Exception(f"Error executing query {query}: {e}")

    def is_json_column_contains_key_and_value(
        self, table_name: str, key: str, value: str
    ) -> bool:
        """Check if a specific key-value pair exists within a JSON column in the given table."""
        try:
            c = self.connection.cursor()
            # Properly format the LIKE clause with escaped quotes for key and value
            c.execute(
                f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE metadata LIKE ?
            LIMIT 1
            """,
                (f'%"{key}": "{value}"%',),
            )
            # Check if the count is greater than 0, indicating the key-value pair exists
            return c.fetchone()[0] > 0
        except OperationalError as e:
            print(f"Error: {e}")
            return False

    def is_identifier_exit(self, table_name: str, value: str) -> bool:
        """Check if a specific value exists within a column in the given table."""
        try:
            c = self.connection.cursor()
            c.execute(
                f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE object_id LIKE ?
            """,
                (value,),
            )
            return c.fetchone()[0] > 0
        except OperationalError as e:
            print(f"Error: {e}")

    def close(self):
        """Close the database connection."""
        self.connection.close()



