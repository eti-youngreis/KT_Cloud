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

# Shared setup fixture for both controllers
@pytest.fixture(scope="module")
def db_setup():
    # Initialize all necessary components
    db_instance_manager = DBInstanceManager("test_mng_table.sqlite")
    db_instance_service = DBInstanceService(db_instance_manager)
    yield db_instance_service
    time.sleep(0.1)

# Fixture for DBInstanceController
@pytest.fixture(scope="class")
def db_instance_controller(db_setup):
    return DBInstanceController(db_setup)

# Fixture for DBSnapshotController
@pytest.fixture(scope="function")
def db_snapshot_controller(db_setup):
    return DBSnapshotController(db_setup)
