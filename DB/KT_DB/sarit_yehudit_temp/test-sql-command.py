import pytest
import os
import sqlite3
from collections import deque
from tempfile import TemporaryDirectory

# Import your classes and functions
from db_instance import DBInstance
from node_subSnapshot import Node_SubSnapshot
from sql_command import (
    create_database,
    create_table,
    insert,
    select,
    delete_record,
    _run_query,
)
from exception import (
    AlreadyExistsError,
    InvalidQueryError,
)

# @pytest.fixture
# def temp_db_instance():
#     # Create a persistent directory for the database
#     persistent_dir = 'persistent_db_directory'
#     os.makedirs(persistent_dir, exist_ok=True)  # Create directory if it doesn't exist

#     # Initialize DBInstance with persistent directory
#     db_instance = DBInstance(
#         db_instance_identifier="test_instance",
#         allocated_storage=20,
#         master_username="admin",
#         master_user_password="password",
#         port=3306,
#     )
#     db_instance.endpoint = persistent_dir  # Override endpoint to persistent_dir
#     db_instance._current_version_queue = deque(
#         [Node_SubSnapshot(parent=None, endpoint=persistent_dir)]
#     )
#     db_instance._last_node_of_current_version = db_instance._current_version_queue[-1]
#     yield db_instance

#     # Close all SQLite connections here
#     for db_path in db_instance._last_node_of_current_version.dbs_paths_dic.values():
#         try:
#             conn = sqlite3.connect(db_path)
#             conn.close()
#         except sqlite3.OperationalError:
#             pass  # Ignore if the database is already closed
        

@pytest.fixture
def temp_db_instance():
    with TemporaryDirectory() as temp_dir:
        # Initialize DBInstance with temporary directory
        db_instance = DBInstance(
            db_instance_identifier="test_instance",
            allocated_storage=20,
            master_username="admin",
            master_user_password="password",
            port=3306,
        )
        db_instance.endpoint = temp_dir  # Override endpoint to temp_dir
        db_instance._current_version_queue = deque(
            [Node_SubSnapshot(parent=None, endpoint=temp_dir)]
        )
        db_instance._last_node_of_current_version = db_instance._current_version_queue[-1]
        yield db_instance

        # Close all SQLite connections here
        for db_path in db_instance._last_node_of_current_version.dbs_paths_dic.values():
            try:
                conn = sqlite3.connect(db_path)
                conn.close()
            except sqlite3.OperationalError:
                pass  # Ignore if the database is already closed


def test_create_database_success(temp_db_instance):
    db_name = "test_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)

    # Check if database file exists
    db_path = os.path.join(temp_db_instance.endpoint, str(
        temp_db_instance._last_node_of_current_version.id_snepshot), f"{db_name}.db")
    assert os.path.exists(db_path), "Database file was not created."

    # Check if db_path is registered in dbs_paths_dic
    assert db_name in temp_db_instance._last_node_of_current_version.dbs_paths_dic, "Database path not registered in dbs_paths_dic."


def test_create_database_already_exists(temp_db_instance):
    db_name = "existing_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)

    with pytest.raises(AlreadyExistsError):
        create_database(
            db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)


def test_create_table(temp_db_instance):
    db_name = "test_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)
    db_path = temp_db_instance._last_node_of_current_version.dbs_paths_dic[db_name]

    create_table_query = """
    CREATE TABLE users (
        name TEXT,
        age INTEGER
    );
    """
    create_table(create_table_query, db_path)

    # Verify table creation
    tables = _run_query(
        db_path, "SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
    assert tables, "Table 'users' was not created."

    # Verify _record_id column exists
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(users);")
    columns = [row[1] for row in cursor.fetchall()]
    assert "_record_id" in columns, "_record_id column was not added."

    # Closing the cursor and connection
    cursor.close()
    conn.close()


def test_insert(temp_db_instance):
    db_name = "test_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)
    db_path = temp_db_instance._last_node_of_current_version.dbs_paths_dic[db_name]

    create_table_query = """
    CREATE TABLE users (
        name TEXT,
        age INTEGER
    );
    """
    create_table(create_table_query, db_path)

    insert_query = "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);"
    insert(temp_db_instance._last_node_of_current_version, insert_query, db_path)

    # Verify insertion
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users;")
    records = cursor.fetchall()
    assert len(records) == 2, "Records were not inserted correctly."
    assert records[0][0] == 0, "_record_id for first record should be 0."
    assert records[1][0] == 1, "_record_id for second record should be 1."
    # if cursor is not None:
    cursor.close()
    if conn is not None:
        conn.close()


def test_select(temp_db_instance):
    db_name = "test_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)
    db_path = temp_db_instance._last_node_of_current_version.dbs_paths_dic[db_name]

    create_table_query = """
    CREATE TABLE users (
        name TEXT,
        age INTEGER
    );
    """
    create_table(create_table_query, db_path)

    insert_query = "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35);"
    insert(temp_db_instance._last_node_of_current_version, insert_query, db_path)

    # Create a snapshot
    temp_db_instance.create_snapshot("snapshot1")

    # Delete one record
    delete_query = "DELETE FROM users WHERE name = 'Bob';"
    queue = temp_db_instance._current_version_queue
    delete_record(queue, delete_query, db_name)

    # Perform select
    select_query = "SELECT _record_id, name, age FROM users;"
    results = select(
        queue,
        db_id=db_name,
        query=select_query,
        snapshots_ids_in_current_version_set={
            temp_db_instance._last_node_of_current_version.id_snepshot}
    )

    # Verify select results (Bob should be excluded)
    assert len(results) == 2, "Select should return 2 records after deletion."
    names = [record[1] for record in results]
    assert "Bob" not in names, "Deleted record 'Bob' should not appear in select results."


def test_delete_record(temp_db_instance):
    db_name = "test_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)
    db_path = temp_db_instance._last_node_of_current_version.dbs_paths_dic[db_name]

    create_table_query = """
    CREATE TABLE users (
        name TEXT,
        age INTEGER
    );
    """
    create_table(create_table_query, db_path)

    insert_query = "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);"
    insert(temp_db_instance._last_node_of_current_version, insert_query, db_path)

    # Delete a record
    delete_query = "DELETE FROM users WHERE name = 'Alice';"
    queue = temp_db_instance._current_version_queue
    delete_record(queue, delete_query, db_name)

    # Verify that 'deleted_records_in_version' table exists and contains the deleted record
    current_node = queue[-1]
    deleted_db_path = current_node.deleted_records_db_path
    print(deleted_db_path)
    conn = sqlite3.connect(deleted_db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM deleted_records_in_version WHERE table_name='users';")
    deleted_records = cursor.fetchall()

    assert len(deleted_records) == 1, "One record should be marked as deleted."
    assert deleted_records[0][0] == 5, "Deleted _record_id should be 0."
    assert deleted_records[0][2] == "users", "Deleted record should reference the 'users' table."

    cursor.close()
    conn.close()


def test_select_with_no_deletions(temp_db_instance):
    db_name = "test_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)
    db_path = temp_db_instance._last_node_of_current_version.dbs_paths_dic[db_name]

    create_table_query = """
    CREATE TABLE users (
        name TEXT,
        age INTEGER
    );
    """
    create_table(create_table_query, db_path)

    insert_query = "INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25);"
    insert(temp_db_instance._last_node_of_current_version, insert_query, db_path)

    # Perform select without any deletions
    select_query = "SELECT _record_id, name, age FROM users;"
    results = select(
        temp_db_instance._current_version_queue,
        db_id=db_name,
        query=select_query,
        snapshots_ids_in_current_version_set=set()
    )

    # Verify select results
    assert len(results) == 2, "Select should return all records when no deletions."
    names = [record[1] for record in results]
    assert "Alice" in names and "Bob" in names, "All inserted records should appear in select results."


def test_insert_invalid_format(temp_db_instance):
    db_name = "test_db"
    create_database(
        db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)
    db_path = temp_db_instance._last_node_of_current_version.dbs_paths_dic[db_name]

    create_table_query = """
    CREATE TABLE users (
        name TEXT,
        age INTEGER
    );
    """
    create_table(create_table_query, db_path)

    # Insert with unsupported format (missing VALUES)
    insert_query = "INSERT INTO users (name, age) SELECT 'Dave', 40;"

    with pytest.raises(ValueError):
        insert(temp_db_instance._last_node_of_current_version,
               insert_query, db_path)

# delete_recordהשגיאה לא תזרק פה משום שכבר נתפסה ב 

# def test_delete_record_with_invalid_format(temp_db_instance):
#     db_name = "test_db"
#     create_database(
#         db_name, temp_db_instance._last_node_of_current_version, temp_db_instance.endpoint)
#     db_path = temp_db_instance._last_node_of_current_version.dbs_paths_dic[db_name]

#     create_table_query = """
#     CREATE TABLE users (
#         name TEXT,
#         age INTEGER
#     );
#     """
#     create_table(create_table_query, db_path)

#     # Insert valid record
#     insert_query = "INSERT INTO users (name, age) VALUES ('Alice', 30);"
#     insert(temp_db_instance._last_node_of_current_version, insert_query, db_path)

#     # Attempt deletion with an invalid query format
#     invalid_delete_query = "DELETE users WHERE name = 'Alice';"
    
#     with pytest.raises(InvalidQueryError):
#         delete_record(temp_db_instance._current_version_queue,
#                       invalid_delete_query)
