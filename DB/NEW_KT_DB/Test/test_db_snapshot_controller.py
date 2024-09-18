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
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import DbSnapshotIdentifierNotFoundError


# Tests for DBSnapshotController
class TestDBSnapshotController:
    def test_create_snapshot(self, db_instance_controller, db_snapshot_controller):
        # Test creating a snapshot of an existing DB instance
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        snapshots = db_snapshot_controller.list_snapshots("test-instance")
        assert "test-snapshot" in snapshots

    def test_delete_snapshot(self, db_instance_controller, db_snapshot_controller):
        # Test deleting an existing snapshot
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        db_snapshot_controller.delete_snapshot("test-instance", "test-snapshot")
        snapshots = db_snapshot_controller.list_snapshots("test-instance")
        assert "test-snapshot" not in snapshots

    def test_restore_snapshot(self, db_instance_controller, db_snapshot_controller):
        # Test restoring a DB instance from a snapshot
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        db_instance_controller.modify_db_instance("test-instance", allocated_storage=20)
        db_snapshot_controller.restore_snapshot("test-instance", "test-snapshot")
        restored_instance = db_instance_controller.get_db_instance("test-instance")
        assert restored_instance.allocated_storage == 10

    def test_create_snapshot_non_existent_instance(self, db_snapshot_controller):
        # Test creating a snapshot for a non-existent DB instance
        with pytest.raises(ValueError):
            db_snapshot_controller.create_snapshot("non-existent-instance", "test-snapshot")

    def test_delete_non_existent_snapshot(self, db_instance_controller, db_snapshot_controller):
        # Test deleting a non-existent snapshot
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        with pytest.raises(DbSnapshotIdentifierNotFoundError):
            db_snapshot_controller.delete_snapshot("test-instance", "non-existent-snapshot")

    def test_restore_to_non_existent_instance(self, db_instance_controller, db_snapshot_controller):
        # Test restoring a snapshot to a non-existent DB instance
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        db_instance_controller.delete_db_instance("test-instance")
        with pytest.raises(ValueError):
            db_snapshot_controller.restore_snapshot("non-existent-instance", "test-snapshot")

    def test_snapshot_limit(self, db_instance_controller, db_snapshot_controller):
        # Test creating snapshots up to and beyond the limit
        db_instance_controller.create_db_instance(db_instance_identifier="test-instance")
        for i in range(50):  # Assuming a limit of 50 snapshots
            db_snapshot_controller.create_snapshot("test-instance", f"test-snapshot-{i}")
        with pytest.raises(ValueError):
            db_snapshot_controller.create_snapshot("test-instance", "one-too-many")
