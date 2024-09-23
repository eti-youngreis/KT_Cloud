import sqlite3
import shutil
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..","..")))
from typing import Dict, Optional
from datetime import datetime
from DB.NEW_KT_DB.Models.DBSnapshotModelNaive import SnapshotNaive
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from DB.NEW_KT_DB.Validation.DBSnapshotValidationsNaive import is_valid_db_instance_id, is_valid_db_snapshot_description, is_valid_progress
from DB.NEW_KT_DB.DataAccess.DBSnapshotManagerNaive import DBSnapshotManagerNaive

class DBSnapshotServiceNaive(DBO):
    def __init__(self, dal: DBSnapshotManagerNaive):
        self.dal = dal
    
    def create(self, db_instance_identifier: str, description: str, progress: str):
        '''Create a new DBSnapshot.'''
        # Validate parameters\
        print('Enter to create')
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

        db_object = self.dal.describeDBSnapshot(db_instance_identifier)
        # Define the file paths for snapshot
        # db_object = describe(db_instance_identifier)

    

        db_instance_directory = db_object.BASE_PATH + '\\' + db_object.endpoint
        snapshot_db_path = f"../snapshot/{db_instance_identifier}_{current_timestamp}.db"
        if not os.path.exists("../snapshot/"):
            os.makedirs("../snapshot/")

        self.db_snapshot = SnapshotNaive(db_instance_identifier, creation_date=datetime.now(), owner_alias=owner_alias, status='inital',
                                    description=description, progress=progress, url_snapshot=snapshot_db_path)

        assert self.db_snapshot is not None, "SnapshotNaive object was not created."

        # shutil.copytree(db_instance_directory, snapshot_db_path)
        print('!--!!!!!!!!!!!!--!!!!!!!!!!!!--!!!!!!!!!!')

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

    def describe(self, db_instance_identifier: str):
        '''Describe the details of the DB snapshot.'''
        return self.dal.describeDBSnapshot(db_instance_identifier)

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
