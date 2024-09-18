import pytest
from tempfile import TemporaryDirectory
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
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
