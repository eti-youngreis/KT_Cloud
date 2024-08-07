import sqlite3
import os

def insert_into_management_table(conn ,class_name, object_id, metadata, parent_id=None):
    """Insert a new object into the management table."""
    # conn = sqlite3.connect('object_management')
    c = conn.cursor()
    c.execute(f'''SELECT * FROM object_management WHERE type_object = ? and object_id = ?''', (class_name, object_id))
    row = c.fetchall()
    if row:
        update_management_table(conn, "object_id", object_id, "metadata", metadata)
        # update_management_table(conn, column_name, column_value,to_update_column, new_value):
    else:
        c.execute('''
            INSERT INTO object_management (type_object, object_id, metadata)
            VALUES (?, ?, ?)
            ''', (class_name, object_id, metadata))
    conn.commit()
    # conn.close()


# def update_management_table(conn, object_id, new_metadata):
#     """Update the metadata of an existing object in the management table."""
#     # conn = sqlite3.connect('object_management')
#     c = conn.cursor()
#     c.execute('''
#               UPDATE object_management
#               SET metadata = ?
#               WHERE object_id = ?
#               ''', (new_metadata, object_id))
#     conn.commit()
    # conn.close()

def update_management_table(conn, column_name, column_value,to_update_column, new_value):
    """Update the metadata of an existing object in the management table."""
    # conn = sqlite3.connect('object_management')
    c = conn.cursor()
    c.execute(f'''
              UPDATE object_management
              SET {to_update_column} = ?
              WHERE {column_name} = ?
              ''', ( new_value, column_value))
    conn.commit()

def update_column_in_management_table(conn, column_name, currentValue,  newValue):
    c = conn.cursor()
    c.execute(f'''
              UPDATE object_management
              SET {column_name} = ?
              WHERE {column_name} = ?
              ''', (newValue, currentValue))
    conn.commit()

def return_object_by_identifier(conn, identifier):
    # try:
        c = conn.cursor()
        c.execute(f'''
        SELECT * FROM object_management
        WHERE object_id LIKE ?
        ''', (identifier,))
        return c.fetchone()
    
    # except OperationalError as e:
    #     print(f"Error: {e}")
    # return True

