import sqlite3
import json

class DataAccessLayer:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def insert(self, table_name: str, object_type: str, metadata: dict) -> None:
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

    def update(self, table_name: str, object_id: int, metadata: dict) -> None:
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

    def select(self, table_name: str, object_type: str, criteria: str) -> list:
        try:
            c = self.conn.cursor()
            c.execute(f'''
            SELECT object_id, metadata FROM {table_name} 
            WHERE type_object = ? AND metadata LIKE ?
            ''', (object_type, criteria))
            return c.fetchall()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error selecting from {table_name}: {e}")

    def delete(self, table_name: str, object_id: int) -> None:
        try:
            c = self.conn.cursor()
            c.execute(f'''
            DELETE FROM {table_name}
            WHERE object_id = ?
            ''', (object_id,))
            self.conn.commit()
        except sqlite3.OperationalError as e:
            raise Exception(f"Error deleting from {table_name}: {e}")
