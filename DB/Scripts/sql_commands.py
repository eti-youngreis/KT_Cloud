
import sqlite3
import os

def insert_into_management_table(conn ,class_name, object_id, metadata, parent_id=None):
    """Insert a new object into the management table."""
    # conn = sqlite3.connect('object_management')
    c = conn.cursor()
    c.execute(f'''SELECT * FROM object_management WHERE type_object = ? and object_id = ?''', (class_name, object_id))
    row = c.fetchall()
    if row:
        update_management_table(conn, object_id, metadata)
    else:
        c.execute('''
            INSERT INTO object_management (type_object, object_id, metadata)
            VALUES (?, ?, ?)
            ''', (class_name, object_id, metadata))
    conn.commit()
    # conn.close()


def update_management_table(conn, object_id, new_metadata):
    """Update the metadata of an existing object in the management table."""
    # conn = sqlite3.connect('object_management')
    c = conn.cursor()
    c.execute('''
              UPDATE object_management
              SET metadata = ?
              WHERE object_id = ?
              ''', (new_metadata, object_id))
    conn.commit()
    # conn.close()