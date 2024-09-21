"""
This class contains tests for various SQL commands and database operations using the SQLCommandHelper class.

The tests cover the following functionality:
- Cloning a database schema
- Running SQL queries
- Creating a new table
- Inserting data into a table
- Selecting data from a table
- Deleting records from a table
- Creating a new database
- Handling database creation errors
- Handling invalid SQL queries

These tests ensure the correct behavior of the SQLCommandHelper class and the underlying database operations.
"""
import pytest
import sqlite3
import os
from DB.NEW_KT_DB.Service.Classes.SDBInstanceService import SQLCommandHelper
from DB.NEW_KT_DB.Exceptions.DBInstanceExceptions import DatabaseCreationError, InvalidQueryError

import tempfile

@pytest.fixture(scope="function")
def temp_db_path():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_db.sqlite")
        yield db_path
    
class TestSQLCommands:
    def test_clone_database_schema(self, temp_db_path):
        # Test cloning a database schema
        source_path = temp_db_path + "_source"
        new_path = temp_db_path + "_new"
        
        # Create a source database with a table
        conn = sqlite3.connect(source_path)
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.close()

        SQLCommandHelper.clone_database_schema(source_path, new_path)

        # Check if the new database has the same schema
        conn = sqlite3.connect(new_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        conn.close()

        assert ('test',) in tables


    def test_create_table(self, temp_db_path):
        # Test creating a new table
        query = "CREATE TABLE test (id INTEGER, name TEXT)"
        SQLCommandHelper.create_table(query, temp_db_path)

        conn = sqlite3.connect(temp_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        conn.close()

        assert ('test',) in tables

    def test_insert(self, temp_db_path):
        # Test inserting data into a table
        conn = sqlite3.connect(temp_db_path)
        conn.execute("CREATE TABLE test (_record_id INTEGER,id INTEGER PRIMARY KEY, name TEXT)")
        conn.close()

        class MockNode:
            def __init__(self):
                self.dbs_paths_dic = {'test_db': temp_db_path}

        mock_node = MockNode()
        query = "INSERT INTO test (name) VALUES ('test_name')"
        SQLCommandHelper.insert(mock_node, query, temp_db_path)

        result = SQLCommandHelper._run_query(temp_db_path, "SELECT * FROM test")
        assert result == [(0,1, 'test_name')]

    def test_select(self, db_instance_controller, db_snapshot_controller):
        # Create a test instance
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance-for-select", allocated_storage=10)

        # Create the database
        db_instance_controller.execute_query("test-instance-for-select", "CREATE DATABASE test-instance-for-select", "test-instance-for-select")

        # Create a table
        create_table_query = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
        db_instance_controller.execute_query("test-instance-for-select", create_table_query, "test-instance-for-select")

        # Insert data
        insert_query = "INSERT INTO test (name) VALUES ('test_name')"
        db_instance_controller.execute_query("test-instance-for-select", insert_query, "test-instance-for-select")

        # Create a snapshot
        db_snapshot_controller.create_snapshot("test-instance-for-select", "test-snapshot")

        # Perform select
        select_query = "SELECT * FROM test"
        result = db_instance_controller.execute_query("test-instance-for-select", select_query, "test-instance-for-select")

        # Assert the result
        assert result == [(0, 1, 'test_name')]

        # Clean up
        db_instance_controller.delete_db_instance("]test-instance-for-select")
        db_snapshot_controller.delete_snapshot("test-instance-for-select", "test-snapshot")

    def test_delete_record(self, temp_db_path):
        # Test deleting a record from a table
        conn = sqlite3.connect(temp_db_path)
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO test (name) VALUES ('test_name')")
        conn.close()

        class MockNode:
            def __init__(self):
                self.dbs_paths_dic = {'test_db': temp_db_path}
                self.deleted_records_db_path = temp_db_path
                self.id_snapshot = 1

        mock_queue = [MockNode()]
        SQLCommandHelper.delete_record(mock_queue, "DELETE FROM test WHERE name='test_name'", 'test_db')

        result = SQLCommandHelper._run_query(temp_db_path, "SELECT * FROM test")
        assert result == []

    def test_create_database(self, temp_db_path):
        # Test creating a new database
        class MockNode:
            def __init__(self):
                self.id_snapshot = 1
                self.dbs_paths_dic = {}

        mock_node = MockNode()
        SQLCommandHelper.create_database('test_db', mock_node, os.path.dirname(temp_db_path))

        assert 'test_db' in mock_node.dbs_paths_dic
        assert os.path.exists(mock_node.dbs_paths_dic['test_db'])

    # def test_database_creation_error(self, temp_db_path):
    #     # Test database creation error handling
    #     class MockNode:
    #         def __init__(self):
    #             self.id_snapshot = 'test_snapshot'
    #             self.dbs_paths_dic = {}

    #     with pytest.raises(DatabaseCreationError):
    #         SQLCommandHelper.create_database('test_db', MockNode(), '')

