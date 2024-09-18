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
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import SQLCommandHelper
from DB.NEW_KT_DB.Exceptions.DBInstanceExceptions import SQLCommandHelper, DatabaseCreationError, InvalidQueryError

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

    def test_run_query(self, temp_db_path):
        # Test running a SQL query
        conn = sqlite3.connect(temp_db_path)
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO test (name) VALUES ('test_name')")
        conn.close()

        result = SQLCommandHelper._run_query(temp_db_path, "SELECT * FROM test")
        assert result == [(1, 'test_name')]

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
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.close()

        class MockNode:
            def __init__(self):
                self.dbs_paths_dic = {'test_db': temp_db_path}

        mock_node = MockNode()
        query = "INSERT INTO test (name) VALUES ('test_name')"
        SQLCommandHelper.insert(mock_node, query, 'test_db')

        result = SQLCommandHelper._run_query(temp_db_path, "SELECT * FROM test")
        assert result == [(1, 'test_name')]

    def test_select(self, temp_db_path):
        # Test selecting data from a table
        conn = sqlite3.connect(temp_db_path)
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO test (name) VALUES ('test_name')")
        conn.close()

        class MockNode:
            def __init__(self):
                self.dbs_paths_dic = {'test_db': temp_db_path}
                self.deleted_records_db_path = temp_db_path

        mock_queue = [MockNode()]
        result = SQLCommandHelper.select(mock_queue, 'test_db', "SELECT * FROM test", set())
        assert result == [(1, 'test_name')]

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

    def test_database_creation_error(self, temp_db_path):
        # Test database creation error handling
        with pytest.raises(DatabaseCreationError):
            SQLCommandHelper.create_database('', None, '')

    def test_invalid_query(self, temp_db_path):
        # Test handling of invalid SQL queries
        with pytest.raises(InvalidQueryError):
            SQLCommandHelper._run_query(temp_db_path, "INVALID SQL QUERY")