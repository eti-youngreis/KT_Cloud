import sqlite3
import shutil
import os
from typing import Dict, Optional
from datetime import datetime
from DB.NEW_KT_DB.DataAccess import DBSnapshotManager
from Models import Snapshot  # Changed to match your model class name
from Abc import DBO
from DB.NEW_KT_DB.Validation.DBSnapshotValidations import is_valid_db_instance_id, is_valid_db_snapshot_description, is_valid_progress

class DBClusterService(DBO):
    def __init__(self, dal: DBSnapshotManager):
        self.dal = dal
    
    def create(self, db_instance_identifier: str, description: str, progress: str):
        '''Create a new DBSnapshot.'''
        # Validate parameters
        if not is_valid_db_instance_id(db_instance_identifier):
            raise ValueError(f"Invalid db_instance_identifier: {db_instance_identifier}")
        if not is_valid_db_snapshot_description(description):
            raise ValueError(f"Invalid description: {description}")
        if not is_valid_progress(progress):
            raise ValueError(f"Invalid progress: {progress}")

        # Create a timestamp with the current date and time
        current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Get the current username
        owner_alias = os.getlogin()

        # Define the file paths for snapshot
        db_object = describe(db_instance_identifier)

        db_instance_directory = db_object.BASE_PATH + '\\' + db_object.endpoint
        snapshot_db_path = f"../snapshot/{db_instance_identifier}_{current_timestamp}.db"

        self.db_snapshot = Snapshot(db_instance_identifier, creation_date=datetime.now(), owner_alias=owner_alias,
                                    description=description, progress=progress, url_snapshot=snapshot_db_path)

        shutil.copytree(db_instance_directory, snapshot_db_path)

        return self.dal.createInMemoryDBSnapshot()

    def delete(self, snapshot_name: str):
        '''Delete an existing DBCluster.'''
        # Validate snapshot_name
        if not is_valid_db_instance_id(snapshot_name):
            raise ValueError(f"Invalid snapshot_name: {snapshot_name}")

        # Delete physical object
        snapshot_path = f"../snapshot/{snapshot_name}.db"
        if os.path.exists(snapshot_path):
            os.remove(snapshot_path)
        else:
            print(f"Snapshot {snapshot_name} does not exist.")
            # Handle an error

        # Delete from memory
        return self.dal.deleteInMemoryDBSnapshot()

    def describe(self):
        '''Describe the details of DBCluster.'''
        return self.dal.describeDBSnapshot()  # Changed to match the method name in DBSnapshotManager

    def modify(self, owner_alias: Optional[str] = None, status: Optional[str] = None,
               description: Optional[str] = None, progress: Optional[str] = None):
        '''Modify an existing DBCluster.'''
        # Validate parameters
        if description and not is_valid_db_snapshot_description(description):
            raise ValueError(f"Invalid description: {description}")
        if progress and not is_valid_progress(progress):
            raise ValueError(f"Invalid progress: {progress}")

        if owner_alias is not None:
            self.owner_alias = owner_alias
        if status is not None:
            self.status = status
        if description is not None:
            self.description = description
        if progress is not None:
            self.progress = progress

    def get(self):
        '''Get code object.'''
        # Return real-time object
        return self.db_snapshot.to_dict() if self.db_snapshot else None
