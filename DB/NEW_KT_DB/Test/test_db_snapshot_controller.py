import pytest
from DB.NEW_KT_DB.Exceptions.DBInstanceExceptions import DbSnapshotIdentifierNotFoundError

class TestDBSnapshotController:
    @pytest.fixture(autouse=True)
    def setup_db_and_snapshot(self, db_instance_controller, db_snapshot_controller):
        instance = db_instance_controller.create_db_instance(db_instance_identifier="test-instance", allocated_storage=10)
        db_snapshot_controller.create_snapshot("test-instance", "test-snapshot")
        yield instance
        try:
            if db_instance_controller.get_db_instance("test-instance"):
                try:
                    db_snapshot_controller.delete_snapshot("test-instance", "test-snapshot")
                except DbSnapshotIdentifierNotFoundError:
                    pass  # Snapshot already deleted, no need to delete again
        except ValueError:
            pass  # Instance already deleted, no need to clean up
        finally:
            try:
                db_instance_controller.delete_db_instance("test-instance")
            except ValueError:
                pass  # Instance already deleted

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
