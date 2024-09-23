"""
Tests for the DBInstanceController class, which provides an interface for managing database instances.

The tests cover the following functionality:
- Creating a new database instance
- Deleting a database instance
- Describing a database instance
- Modifying a database instance
- Handling invalid input for creating a database instance
- Handling non-existent database instances
- Handling boundary conditions for modifying a database instance
- Stopping and starting a running database instance
"""
import pytest

class TestDBInstanceController:

    @pytest.fixture(autouse=True)
    def setup_db_instance(self, db_instance_controller):
        instance = db_instance_controller.create_db_instance(db_instance_identifier="test-instance", allocated_storage=10)
        yield instance
        db_instance_controller.delete_db_instance("test-instance")
    
    def test_create_db_instance(self, setup_db_instance):
        assert setup_db_instance.db_instance_identifier == "test-instance"
        assert setup_db_instance.allocated_storage == 10

    def test_delete_db_instance(self, db_instance_controller):
        db_instance_controller.delete_db_instance("test-instance")
        with pytest.raises(Exception) as excinfo:
            db_instance_controller.get_db_instance("test-instance")
        assert "Error occurred while fetching data from table" in str(excinfo.value)    

    def test_describe_db_instance(self, db_instance_controller):
        description = db_instance_controller.describe_db_instance("test-instance")
        assert description["db_instance_identifier"] == "test-instance"

    def test_modify_db_instance(self, db_instance_controller):
        modified_instance = db_instance_controller.modify_db_instance("test-instance", allocated_storage=20)
        assert modified_instance.allocated_storage == 20

    def test_create_db_instance_invalid_input(self, db_instance_controller):
        with pytest.raises(ValueError):
            db_instance_controller.create_db_instance(db_instance_identifier="", allocated_storage=-1)

    def test_get_non_existent_db_instance(self, db_instance_controller):
        with pytest.raises(Exception) as excinfo:
            db_instance_controller.get_db_instance("non-existent-instance")
        assert "Error occurred while fetching data from table" in str(excinfo.value)


    def test_modify_db_instance_boundary(self, db_instance_controller):
        with pytest.raises(ValueError):
            db_instance_controller.modify_db_instance("test-instance", allocated_storage=0)

    def test_stop_running_instance(self, db_instance_controller):
        db_instance_controller.stop_db_instance("test-instance")
        instance = db_instance_controller.get_db_instance("test-instance")
        assert instance.status == 'stopped'

    def test_start_stopped_instance(self, db_instance_controller):
        db_instance_controller.stop_db_instance("test-instance")
        db_instance_controller.start_db_instance("test-instance")
        instance = db_instance_controller.get_db_instance("test-instance")
        assert instance.status == 'available'