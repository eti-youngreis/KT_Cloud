from Service import DBSnapshotService
from DB.NEW_KT_DB.Validation.DBSnapshotValidationsNaive import (
    is_valid_db_instance_id, 
    is_valid_db_snapshot_description, 
    is_valid_progress
)
class DBSnapshotControllerNaive:
    def create_db_snapshot(self, db_instance_identifier: str, description: str = None, progress: str = None):
        # Validate parameters
        if not is_valid_db_instance_id(db_instance_identifier):
            raise ValueError(f"Invalid db_name: {db_instance_identifier}")
        if description and not is_valid_db_snapshot_description(description):
            raise ValueError(f"Invalid description: {description}")
        if progress and not is_valid_progress(progress):
            raise ValueError(f"Invalid progress: {progress}")

        self.service.create(db_instance_identifier, description, progress)

    def delete_db_snapshot(self):
        # No validation needed for delete operation
        self.service.delete()

    def modify_db_snapshot(self, owner_alias: str = None, status: str = None,
                           description: str = None, progress: str = None):
        # Validate parameters
        if description and not is_valid_db_snapshot_description(description):
            raise ValueError(f"Invalid description: {description}")
        if progress and not is_valid_progress(progress):
            raise ValueError(f"Invalid progress: {progress}")

        self.service.modify(owner_alias, status, description, progress)
