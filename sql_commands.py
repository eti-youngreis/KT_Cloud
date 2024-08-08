import sqlite3
import os
MANAGEMENT_DB = 'object_management.db'


def start_object_management_table(table_name):
    """Create the management table if it doesn't exist."""
    table_schema = 'class_name TEXT,object_id TEXT,metadata TEXT,parent_id TEXT'
    create_table(MANAGEMENT_DB, table_name, table_schema)


def insert_into_management_table(class_name, object_id, metadata, parent_id=None):
    """Insert a new object into the management table."""
    select_query = f"SELECT * FROM object_management WHERE class_name = '{class_name}' and object_id = '{object_id}'"
    #check if object id exists in row of table
    row = execute_query(MANAGEMENT_DB,select_query,None,False) 
    if row: #if exist
        update_management_table(object_id, metadata)#update the row of object id 
    else: #if not exist
        #insert new row with object id as id
        data = {'class_name':class_name, 'object_id':object_id, 'metadata':metadata, 'parent_id':parent_id}
        insert_into_table(MANAGEMENT_DB,'object_management',data)



def update_management_table(object_id, new_metadata):
    """Update the metadata of an existing object in the management table."""
    update_query = f"UPDATE object_management SET metadata = '{new_metadata}' WHERE object_id = '{object_id}'" 
    execute_query(MANAGEMENT_DB,update_query)

def create_db(db_path):
    """Create a new SQLite database at the specified path."""
    if not os.path.exists(db_path):
        new_conn = sqlite3.connect(db_path)
        new_conn.close()

def create_table(db_name, table_name, table_schema):
    "create a table in a given db by given table_schema"
    query = f'CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})'
    execute_query(db_name, query)


def insert_into_table(db, table, data):
    """insert data to row in table"""
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?'] * len(data))
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    execute_query(db, query, tuple(data.values()))


def check_if_object_exists_in_table(db, table, object_id):
    """Check if an object exists in the specified table."""
    res = get_object_from_table_by_id(db, table, object_id)
    return res is not None


def get_object_from_table_by_id(db, table, object_id):
    query = f"SELECT * FROM {table} WHERE object_id = '{object_id}'"
    res = execute_query(db, query, None, False)
    return res


def get_objects_by_class_name(db, table, class_name):
    query = f"SELECT * FROM {table} WHERE class_name = '{class_name}'"
    res = execute_query(db, query, None, True)
    return res


def del_object(object_id, table, db):
    """Delete an object from the specified table."""
    query = f"DELETE FROM {table} WHERE object_id == '{object_id}'"
    execute_query(db, query)


def del_class_from_management_table(class_name):
    """delete rows from management table by class name"""
    query = f"DELETE FROM object_management WHERE class_name = '{class_name}'"
    execute_query('object_management.db', query)


def execute_query(db_name, query, params=None, return_all_answers_to_query_flag=True):
    """Execute a query on the specified database"""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    if params:
        c.execute(query, params)
    else:
        c.execute(query)
    res = c.fetchall() if return_all_answers_to_query_flag else c.fetchone()
    conn.commit()
    conn.close()
    return res
