import sqlite3
import shutil
import os
from typing import Dict, Optional
from datetime import datetime
from DB.NEW_KT_DB.DataAccess import DBSnapshotManager
from Models import DBSnapshotModel
from Abc import DBO
from Validation import Validation
from DataAccess import DBClusterManager
from DB.NEW_KT_DB.Service.Classes.DBInstanceService import describe

class DBClusterService(DBO):
    def __init__(self, dal: DBSnapshotManager):
        self.dal = dal
    
    # validations here

    def create(self, db_instance_identifier: str, description: str, progress: str):
        '''Create a new DBSnapshot.'''
        # Create a timestamp with the current date and time
        current_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # Get the current username
        owner_alias = os.getlogin()

        # Define the file paths for snapshot

        db_object = describe(db_instance_identifier)

        db_instance_directory = db_object.BASE_PATH + '\\' + db_object.endpoint


        snapshot_db_path = f"../snapshot/{db_instance_identifier}_{current_timestamp}.db"

        self.db_snapshot = DBSnapshotModel(db_instance_identifier, creation_date = current_timestamp, owner_alias = owner_alias,
                                      description = description, progress = progress, url_snapshot = snapshot_db_path)
        
        # שכפול תיקיית ה-DBINSTANCE לתיקיית ה-SNAPSHOT
        shutil.copytree(db_instance_directory, snapshot_db_path)
        
        return self.dal.createInMemoryDBSnapshot()


    def delete(self, snapshot_name):
        '''Delete an existing DBCluster.'''
        # delete physical object
        snapshot_path = f"../snapshot/{snapshot_name}.db"
        if os.path.exists(snapshot_path):
            os.remove(snapshot_path)
        else:
            print(f"Snapshot {snapshot_name} does not exist.")
            # Handle an error

        # delete from memory using
        return self.dal.deleteInMemoryDBCluster()

    def describe(self):
        '''Describe the details of DBCluster.'''
        return self.dal.describeDBCluster()      

    def modify(self, owner_alias: str, status: str, description: str,
               progress: str):
        '''Modify an existing DBCluster.'''
        # update object in code
        # modify physical object
        # update object in memory using DBClusterManager.modifyInMemoryDBCluster() function- send criteria using self attributes
        
        if owner_alias is not None:
            self.owner_alias = owner_alias
        if status is not None:
            self.status = status
        if description is not None:
            self.description = description
        if progress is not None:
            self.progress = progress

    def get(self):
        '''get code object.'''
        # return real time object
        return self.db_snapshot.to_dict()
