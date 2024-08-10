from typing import Dict, Any
import json
import sqlite3

class GenericData:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert(self, table_name: str, object_type: str, metadata: Dict[str, Any]) -> None:
        """Insert a new record into the specified table."""
        metadata_json = json.dumps(metadata)
        try:
            c = self.conn.cursor()
            c.execute(f'''
            INSERT INTO {table_name} (type_object, metadata)
            VALUES (?, ?)
            ''', (object_type, metadata_json))
            self.conn.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error inserting into {table_name}: {e}")

    def update(self, table_name: str, object_id: int, metadata: Dict[str, Any]) -> None:
        """Update an existing record in the specified table."""
        metadata_json = json.dumps(metadata)
        try:
            c = self.conn.cursor()
            c.execute(f'''
            UPDATE {table_name}
            SET metadata = ?
            WHERE object_id = ?
            ''', (metadata_json, object_id))
            self.conn.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error updating {table_name}: {e}")

    def select(self, table_name: str, criteria: str) -> Dict[str, Any]:
        """Select records from the specified table based on criteria."""
        try:
            c = self.conn.cursor()
            c.execute(f'''
            SELECT object_id, metadata FROM {table_name} 
            WHERE metadata LIKE ?
            ''', (criteria,))
            results = c.fetchall()
            return {result[0]: json.loads(result[1]) for result in results}
        except sqlite3.OperationalError as e:
            raise Exception(f"Error selecting from {table_name}: {e}")

    def delete(self, table_name: str, object_id: int) -> None:
        """Delete a record from the specified table."""
        try:
            c = self.conn.cursor()
            c.execute(f'''
            DELETE FROM {table_name}
            WHERE object_id = ?
            ''', (object_id,))
            self.conn.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error deleting from {table_name}: {e}")
