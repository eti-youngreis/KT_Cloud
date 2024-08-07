import sqlite3
import os



def start_object_management_table(table_name):
    """Create the management table if it doesn't exist."""
    table_schema = 'class_name TEXT,object_id TEXT,metadata TEXT,parent_id TEXT'
    create_table('object_management.db', table_name, table_schema)


def insert_into_management_table(class_name, object_id, metadata, parent_id=None):
    """Insert a new object into the management table."""
    select_query = f'''SELECT * FROM object_management WHERE class_name = ? and object_id = ?'''
    select_params = (class_name, object_id)
    #check if object id exists in row of table
    row = execute_query('object_management.db',select_query,select_params,False) 
    if row: #if exist
        update_management_table(object_id, metadata)#update the row of object id 
    else: #if not exist
        #insert new row with object id as id
        data = {'class_name':class_name, 'object_id':object_id, 'metadata':metadata, 'parent_id':parent_id}
        insert_into_table('object_management.db','object_management',data)



def update_management_table(object_id, new_metadata):
    """Update the metadata of an existing object in the management table."""
    update_query = '''UPDATE object_management SET metadata = ? WHERE object_id = ?''' 
    update_params = (new_metadata, object_id)
    execute_query('object_management.db',update_query, update_params)

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
    "insert data to row in table"
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['?'] * len(data))
    query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    execute_query(db, query, tuple(data.values()))


def check_if_exists_in_table(db, table, object_id):
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


def comments():
# def create_change_log_table(db_path):
#     """
#     Create the change_log table in the SQLite database.
#
#     Parameters:
#     db_path (str): The path to the SQLite database file.
#     """
#     connection = sqlite3.connect(db_path)
#     cursor = connection.cursor()
#
#     try:
#         cursor.execute('''
#             CREATE TABLE IF NOT EXISTS change_log (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 table_name TEXT NOT NULL,
#                 operation TEXT NOT NULL,
#                 row_id INTEGER NOT NULL,
#                 old_data TEXT,
#                 new_data TEXT,
#                 change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#             )
#         ''')
#         connection.commit()
#         print("change_log table created successfully.")
#     finally:
#         connection.close()
#
#
# def log_change(db_path, table_name, operation, row_id, old_data, new_data):
#     """
#     Log a change to the change_log table.
#
#     Parameters:
#     db_path (str): The path to the SQLite database file.
#     table_name (str): The name of the table where the change occurred.
#     operation (str): The type of operation (INSERT, UPDATE, DELETE).
#     row_id (int): The ID of the row that was changed.
#     old_data (any): The old data before the change.
#     new_data (any): The new data after the change.
#     """
#     connection = sqlite3.connect(db_path)
#     cursor = connection.cursor()
#
#     try:
#         cursor.execute('''
#             INSERT INTO change_log (table_name, operation, row_id, old_data, new_data)
#             VALUES (?, ?, ?, ?, ?)
#         ''', (table_name, operation, row_id, to_json(old_data), to_json(new_data)))
#         connection.commit()
#     finally:
#         connection.close()
#
#
# def create_triggers_for_table(db_path, table_name):
#     """
#     Create triggers for the specified table in the SQLite database.
#
#     Parameters:
#     db_path (str): The path to the SQLite database file.
#     table_name (str): The name of the table to create triggers for.
#     """
#     connection = sqlite3.connect(db_path)
#     cursor = connection.cursor()
#
#     try:
#         cursor.executescript(f'''
#             CREATE TRIGGER IF NOT EXISTS before_update_{table_name}
#             BEFORE UPDATE ON "{table_name}"
#             FOR EACH ROW
#             BEGIN
#                 SELECT log_change('{db_path}', '{table_name}', 'UPDATE', OLD.id, OLD, NEW);
#             END;
#
#             CREATE TRIGGER IF NOT EXISTS before_insert_{table_name}
#             BEFORE INSERT ON "{table_name}"
#             FOR EACH ROW
#             BEGIN
#                 SELECT log_change('{db_path}', '{table_name}', 'INSERT', NEW.id, NULL, NEW);
#             END;
#
#             CREATE TRIGGER IF NOT EXISTS before_delete_{table_name}
#             BEFORE DELETE ON "{table_name}"
#             FOR EACH ROW
#             BEGIN
#                 SELECT log_change('{db_path}', '{table_name}', 'DELETE', OLD.id, OLD, NULL);
#             END;
#         ''')
#         connection.commit()
#         print(f"Triggers for table {table_name} created successfully.")
#     finally:
#         connection.close()
#
#
#
# def create_triggers_for_all_tables(db_path):
#     """
#     Create triggers for all tables in the SQLite database.
#
#     Parameters:
#     db_path (str): The path to the SQLite database file.
#     """
#     connection = sqlite3.connect(db_path)
#     cursor = connection.cursor()
#
#     try:
#         cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
#         tables = cursor.fetchall()
#
#         for table in tables:
#             table_name = table[0]
#             create_triggers_for_table(db_path, table_name)
#     finally:
#         connection.close()
#
#
# def fetch_changes_since(db_path, since_time):
#     """
#     Fetch changes from the change_log table where change_time is greater than the given since_time.
#
#     Parameters:
#     db_path (str): The path to the SQLite database file.
#     since_time (str): The time in ISO format (YYYY-MM-DD HH:MM:SS) to fetch changes since.
#
#     Returns:
#     list: A list of dictionaries representing the changes.
#     """
#     connection = sqlite3.connect(db_path)
#     cursor = connection.cursor()
#
#     try:
#         query = '''
#             SELECT id, table_name, operation, row_id, old_data, new_data, change_time
#             FROM change_log
#             WHERE change_time > ?
#         '''
#         cursor.execute(query, (since_time,))
#         rows = cursor.fetchall()
#
#         # Convert rows to list of dictionaries
#         changes = []
#         for row in rows:
#             change = {
#                 'id': row[0],
#                 'table_name': row[1],
#                 'operation': row[2],
#                 'row_id': row[3],
#                 'old_data': row[4],
#                 'new_data': row[5],
#                 'change_time': row[6]
#             }
#             changes.append(change)
#
#         return changes
#     finally:
#         connection.close()
    pass

def del_class_from_management_table(class_name):
    query = f'DELETE FROM object_management WHERE class_name = ?'
    params = (class_name,)
    execute_query('object_management.db', query, params)


def execute_query(db_name, query, params=None, want_all=True):
    """Execute a query on the specified database"""
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    if params:
        c.execute(query, params)
    else:
        c.execute(query)
    res = c.fetchall() if want_all else c.fetchone()
    conn.commit()
    conn.close()
    return res
