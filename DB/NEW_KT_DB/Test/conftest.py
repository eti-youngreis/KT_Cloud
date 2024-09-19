import pytest
from tempfile import TemporaryDirectory
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from DB.NEW_KT_DB.Controller.DBInstanceController import DBInstanceController
from DB.NEW_KT_DB.Controller.DBSnapshotController import DBSnapshotController
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DBInstanceService
from DB.NEW_KT_DB.DataAccess.DBInstanceManager import DBInstanceManager
import time

# Fixture to create a temporary database path
@pytest.fixture(scope="module")
def temp_db_path():
    with TemporaryDirectory() as temp_dir:
        db_path = os.path.join(temp_dir, "test_db.sqlite")
        yield db_path
        time.sleep(0.1)
        # Cleanup is handled automatically by TemporaryDirectory

# Shared setup fixture for both controllers
@pytest.fixture(scope="module")
def db_setup(temp_db_path):
    # Initialize all necessary components
    db_instance_manager = DBInstanceManager(temp_db_path)
    db_instance_service = DBInstanceService(db_instance_manager)
    
    yield db_instance_service
    
    db_instance_service.close_connections()
    time.sleep(0.1)

# Fixture for DBInstanceController
@pytest.fixture(scope="function")
def db_instance_controller(db_setup):
    return DBInstanceController(db_setup)

# Fixture for DBSnapshotController
@pytest.fixture(scope="function")
def db_snapshot_controller(db_setup):
    return DBSnapshotController(db_setup)
