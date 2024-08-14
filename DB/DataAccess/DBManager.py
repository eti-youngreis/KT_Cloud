import sqlite3
from typing import Dict, Any
import json

class DBManager:
    def __init__(self, db_file: str):
        """Initialize the database connection and create tables if they do not exist."""
        self.connection = sqlite3.connect(db_file)
        self.create_tables()

    def create_tables(self):
        """Create tables in the database if they do not exist."""
        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS objects (
                    object_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type_object TEXT NOT NULL,
                    metadata TEXT NOT NULL
                )
            """)

    def insert(self, table_name: str, object_type: str, metadata: Dict[str, Any]) -> None:
        """Insert a new record into the specified table."""
        metadata_json = json.dumps(metadata)
        try:
            c = self.connection.cursor()
            c.execute(f'''
                INSERT INTO {table_name} (type_object, metadata)
                VALUES (?, ?)
            ''', (object_type, metadata_json))
            self.connection.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error inserting into {table_name}: {e}")

    def update(self, table_name: str, updates: Dict[str, Any], criteria: str) -> None:
        """Update records in the specified table based on criteria."""
        set_clause = ', '.join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())
        
        try:
            c = self.connection.cursor()
            c.execute(f'''
                UPDATE {table_name}
                SET {set_clause}
                WHERE {criteria}
            ''', values)
            self.connection.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error updating {table_name}: {e}")

    def select(self, table_name: str, criteria: str) -> Dict[int, Dict[str, Any]]:
        """Select records from the specified table based on criteria."""
        try:
            c = self.connection.cursor()
            c.execute(f'''
                SELECT object_id, metadata FROM {table_name} 
                WHERE {criteria}
            ''')
            results = c.fetchall()
            return {result[0]: json.loads(result[1]) for result in results}
        except sqlite3.OperationalError as e:
            raise Exception(f"Error selecting from {table_name}: {e}")

    def delete(self, table_name: str, criteria: str) -> None:
        """Delete a record from the specified table based on criteria."""
        try:
            c = self.connection.cursor()
            c.execute(f'''
                DELETE FROM {table_name}
                WHERE {criteria}
            ''')
            self.connection.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error deleting from {table_name}: {e}")

    def close(self):
        """Close the database connection."""
        self.connection.close()
