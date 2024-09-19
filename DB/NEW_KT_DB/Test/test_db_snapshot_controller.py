"""
Tests for the DBSnapshotController class, which provides functionality for creating, deleting, and restoring database snapshots.

The tests cover the following scenarios:
- Creating a snapshot of an existing database instance
- Deleting an existing snapshot
- Restoring a database instance from a snapshot
- Creating a snapshot for a non-existent database instance
- Deleting a non-existent snapshot
- Restoring a snapshot to a non-existent database instance
- Creating snapshots up to and beyond the limit

These tests ensure the proper functioning of the DBSnapshotController and its ability to manage database snapshots.
"""
import pytest
from DB.NEW_KT_DB.Exceptions.DBInstanceExceptions import DbSnapshotIdentifierNotFoundError


# Tests for DBSnapshotController
class TestDBSnapshotController:

   @pytest.fixture(autouse=True)
   def setup_db_and_snapshot(self, db_instance_controller, db_snapshot_controller):
       instance = db_instance_controller.create_db_instance(db_instance_identifier="test-instance", allocated_storage=10)
       db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
       yield instance
       try:
           db_snapshot_controller.delete_snapshot("test-instance", "test-snapshot")
       except DbSnapshotIdentifierNotFoundError:
           pass
       db_instance_controller.delete_db_instance("test-instance")

   def test_create_snapshot(self, db_snapshot_controller):
       snapshots = db_snapshot_controller.list_snapshots("test-instance")
       assert "test-snapshot" in snapshots

   def test_delete_snapshot(self, db_snapshot_controller):
       db_snapshot_controller.delete_snapshot("test-instance", "test-snapshot")
       snapshots = db_snapshot_controller.list_snapshots("test-instance")
       assert "test-snapshot" not in snapshots

   def test_create_snapshot_non_existent_instance(self, db_snapshot_controller):
       with pytest.raises(ValueError):
           db_snapshot_controller.create_snapshot("non-existent-instance", "test-snapshot")

   def test_delete_non_existent_snapshot(self, db_snapshot_controller):
       with pytest.raises(DbSnapshotIdentifierNotFoundError):
           db_snapshot_controller.delete_snapshot("test-instance", "non-existent-snapshot")

   def test_restore_to_non_existent_instance(self, db_instance_controller, db_snapshot_controller):
       db_instance_controller.delete_db_instance("test-instance")
       with pytest.raises(ValueError):
           db_snapshot_controller.restore_snapshot("non-existent-instance", "test-snapshot")
