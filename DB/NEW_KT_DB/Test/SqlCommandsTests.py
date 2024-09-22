import pytest
from DB.NEW_KT_DB.Controller.DBInstanceController import DBInstanceController

class TestSqlCommands:

    @pytest.fixture(scope="class")
    def setup_db_instance(self, db_instance_controller):
        instance = db_instance_controller.create_db_instance(
            db_instance_identifier="test-instance",
            allocated_storage=20,
            master_username="admin",
            master_user_password="password123",
            port=3306
        )
        
        yield instance
        db_instance_controller.delete_db_instance("test-instance")

    def test_execute_query_create_database(self, db_instance_controller, setup_db_instance):
        query = "CREATE DATABASE test_db"
        instance_identifier = setup_db_instance.db_instance_identifier
        result = db_instance_controller.execute_query(instance_identifier, query, "test_db")
        print(result)
        assert result is None

    def test_execute_query_create_table(self, db_instance_controller, setup_db_instance):
        create_table_query = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"
        result = db_instance_controller.execute_query(setup_db_instance.db_instance_identifier, create_table_query, "test_db")
        assert result is None

    def test_execute_query_insert(self, db_instance_controller, setup_db_instance):
        insert_query = "INSERT INTO test_table (name) VALUES ('Test Name')"
        result = db_instance_controller.execute_query(setup_db_instance.db_instance_identifier, insert_query, "test_db")
        assert result is None

    def test_execute_query_select(self, db_instance_controller, setup_db_instance):
        select_query = "SELECT * FROM test_table"
        result = db_instance_controller.execute_query(setup_db_instance.db_instance_identifier, select_query, "test_db")
        assert len(result) == 1
        assert result[0][2] == 'Test Name'

    def test_execute_query_delete(self, db_instance_controller, setup_db_instance):
        delete_query = "DELETE FROM test_table WHERE name = 'Test Name'"
        result = db_instance_controller.execute_query(setup_db_instance.db_instance_identifier, delete_query, "test_db")
        assert result is None

        select_query = "SELECT * FROM test_table"
        result = db_instance_controller.execute_query(setup_db_instance.db_instance_identifier, select_query, "test_db")
        assert len(result) == 0
    
    def test_execute_query_unsupported(self, db_instance_controller, setup_db_instance):
        unsupported_query = "UPDATE test_table SET name = 'New Name' WHERE id = 1"
        with pytest.raises(ValueError, match="Unsupported query type: UPDATE"):
            db_instance_controller.execute_query(setup_db_instance.db_instance_identifier, unsupported_query, "test_db")
