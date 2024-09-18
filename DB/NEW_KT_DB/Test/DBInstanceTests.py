import pytest
import os
from tempfile import TemporaryDirectory
from DB.NEW_KT_DB.Controller.DBInstanceController import DBInstanceController
from DB.NEW_KT_DB.Controller.DBSnapshotController import DBSnapshotController
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceService
from DB.NEW_KT_DB.DataAccess.DBInstanceManager import DBInstanceManager
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.DataAccess.DBManager import DBManager

# Fixture to create a temporary database path
@pytest.fixture(scope="function")
def temp_db_path():
    with TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_db.sqlite")
        yield db_path
        # Cleanup is handled automatically by TemporaryDirectory

# Shared setup fixture for both controllers
@pytest.fixture(scope="function")
def db_setup(temp_db_path):
    # Initialize all necessary components
    db_manager = DBManager(temp_db_path)
    object_manager = ObjectManager(db_manager)
    db_instance_manager = DBInstanceManager(object_manager)
    db_instance_service = DBInstanceService(db_instance_manager)
    
    yield db_instance_service, db_manager
    
    # Cleanup after test
    db_instance_manager.deleteInMemoryDBInstance(temp_db_path)
    db_manager.close_connection()

# Fixture for DBInstanceController
@pytest.fixture(scope="function")
def db_instance_controller(db_setup):
    db_instance_service, _ = db_setup
    return DBInstanceController(db_instance_service)

# Fixture for DBSnapshotController
@pytest.fixture(scope="function")
def db_snapshot_controller(db_setup):
    db_instance_service, _ = db_setup
    return DBSnapshotController(db_instance_service)

# Tests for DBInstanceController
class TestDBInstanceController:
    def test_create_db_instance(self, db_instance_controller):
        # Test creating a new DB instance
        instance = db_instance_controller.create_db_instance(db_instance_identifier="test-instance", allocated_storage=10)
        assert instance.db_instance_identifier == "test-instance"
        assert instance.allocated_storage == 10

    def test_delete_db_instance(self, db_instance_controller):
        # Test deleting a DB instance
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_instance_controller.delete_db_instance("test-instance")
        with pytest.raises(ValueError):
            db_instance_controller.get_db_instance("test-instance")

    def test_describe_db_instance(self, db_instance_controller):
        # Test describing a DB instance
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        description = db_instance_controller.describe_db_instance("test-instance")
        assert description["db_instance_identifier"] == "test-instance"

    def test_modify_db_instance(self, db_instance_controller):
        # Test modifying a DB instance
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance", allocated_storage=10)
        modified_instance = db_instance_controller.modify_db_instance("test-instance", allocated_storage=20)
        assert modified_instance.allocated_storage == 20

# Tests for DBSnapshotController
class TestDBSnapshotController:
    def test_create_snapshot(self, db_instance_controller, db_snapshot_controller):
        # Test creating a snapshot
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        snapshots = db_snapshot_controller.list_snapshots("test-instance")
        assert "test-snapshot" in snapshots

    def test_delete_snapshot(self, db_instance_controller, db_snapshot_controller):
        # Test deleting a snapshot
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        db_snapshot_controller.delete_snapshot("test-instance", "test-snapshot")
        snapshots = db_snapshot_controller.list_snapshots("test-instance")
        assert "test-snapshot" not in snapshots

    def test_restore_snapshot(self, db_instance_controller, db_snapshot_controller):
        # Test restoring from a snapshot
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        db_instance_controller.modify_db_instance("test-instance", allocated_storage=20)
        db_snapshot_controller.restore_snapshot("test-instance", "test-snapshot")
        restored_instance = db_instance_controller.get_db_instance("test-instance")
        assert restored_instance.allocated_storage == 10

if __name__ == "__main__":
    pytest.main()