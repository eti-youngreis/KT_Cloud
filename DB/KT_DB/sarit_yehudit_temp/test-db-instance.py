import pytest
import os
from db_instance import DBInstance
from node_subSnapshot import Node_SubSnapshot
from exception import DbSnapshotIdentifierNotFoundError

@pytest.fixture
def db_instance():
    return DBInstance(
        db_instance_identifier="test_instance",
        allocated_storage=20,
        master_username="admin",
        master_user_password="password",
        db_name="test_db"
    )

def test_db_instance_initialization(db_instance):
    assert db_instance.db_instance_identifier == "test_instance"
    assert db_instance.allocated_storage == 20
    assert db_instance.master_username == "admin"
    assert db_instance.port == 3306
    assert os.path.exists(db_instance.endpoint)

def test_create_snapshot(db_instance):
    snapshot_name = "snapshot_1"
    db_instance.create_snapshot(snapshot_name)
    assert snapshot_name in db_instance._node_subSnapshot_name_to_id

def test_restore_version_success(db_instance):
    snapshot_name = "snapshot_1"
    db_instance.create_snapshot(snapshot_name)
    db_instance.restore_version(snapshot_name)
    assert len(db_instance._current_version_queue) > 1

def test_restore_version_failure(db_instance):
    with pytest.raises(DbSnapshotIdentifierNotFoundError):
        db_instance.restore_version("non_existing_snapshot")
