import os
import uuid
import pytest

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

    @pytest.fixture(scope="class")
    def setup_replica(self, setup_db_instance,db_instance_controller):
        instance = db_instance_controller.create_db_instance_read_replica(
            source_db_instance_identifier=setup_db_instance.db_instance_identifier,
            db_instance_identifier="test-read-replica",
            allocated_storage=20,
            master_username="admin",
            master_user_password="password123",
            port=3306
        )
        
        yield instance
        db_instance_controller.delete_db_instance("test-read-replica")    

    def test_execute_query_create_database(self, setup_replica, db_instance_controller, setup_db_instance, db_snapshot_controller):
        query = "CREATE DATABASE test_db"
        instance_identifier = setup_db_instance.db_instance_identifier
        result = db_instance_controller.execute_query(instance_identifier, query, "test_db")
        db_snapshot_controller.create_snapshot(instance_identifier, "test_snapshot")
         
        # Retrieve the instance again
        db_instance = db_instance_controller.get_db_instance(instance_identifier)
        print('\n\ndb_instance.to_dict():',db_instance.to_dict())
        updated_instance = db_instance_controller.get_db_instance("test-read-replica")
        print('updated_instance.to_dict():',updated_instance.to_dict())
        # Check if the new snapshot exists
        assert "test_snapshot" in updated_instance._node_subSnapshot_name_to_id
        
        # Get the new snapshot's ID
        new_snapshot_id = updated_instance._node_subSnapshot_name_to_id["test_snapshot"]
        
        # Verify the new snapshot exists in _node_subSnapshot_dic
        assert new_snapshot_id in {str(k): v for k, v in updated_instance._node_subSnapshot_dic.items()}
        
        # Check if the database paths are correct
        new_snapshot = updated_instance._node_subSnapshot_dic[uuid.UUID(new_snapshot_id)]
        assert "test_db" in new_snapshot.dbs_paths_dic
        
        # Verify the database file exists
        assert os.path.exists(new_snapshot.dbs_paths_dic["test_db"])
    
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
