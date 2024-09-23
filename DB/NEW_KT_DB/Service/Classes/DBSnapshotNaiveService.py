import sqlite3
import shutil
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..","..")))
from typing import Dict, Optional
from datetime import datetime
from DB.NEW_KT_DB.Models.DBSnapshotNaiveModel import SnapshotNaive
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from DB.NEW_KT_DB.Validation.DBSnapshotNaiveValidations import is_valid_db_instance_id, is_valid_db_snapshot_description, is_valid_progress
from DB.NEW_KT_DB.DataAccess.DBSnapshotNaiveManager import DBSnapshotNaiveManager

class DBSnapshotNaiveService(DBO):
    def __init__(self, dal: DBSnapshotNaiveManager):
        self.dal = dal
    
    def create(self,db_snapshot_identifier: str, db_instance_identifier: str, description: str, progress: str):
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

        db_snapshot = SnapshotNaive(db_snapshot_identifier, db_instance_identifier, creation_date=datetime.now(), owner_alias=owner_alias, status='inital',
                                    description=description, progress=progress, url_snapshot=snapshot_db_path)

        # assert self.db_snapshot is not None, "SnapshotNaive object was not created."

        shutil.copytree(db_instance_directory, snapshot_db_path)

        self.dal.createInMemoryDBSnapshot(db_snapshot)
        return {'DBSnapshot': db_snapshot.to_dict()}

    def delete(self, db_snapshot_identifier: str):
        '''Delete an existing DBCluster.'''
        # Validate snapshot_name
        if not is_valid_db_instance_id(db_snapshot_identifier):
            raise ValueError(f"Invalid snapshot_name: {db_snapshot_identifier}")

        # Delete physical object
        snapshot_path = f"../snapshot/{db_snapshot_identifier}.db"
        db_snapshot = self.get(db_snapshot_identifier)
        if os.path.exists(snapshot_path):
            os.remove(snapshot_path)
        else:
            print(f"Snapshot {db_snapshot_identifier} does not exist.")
            # Handle an error

        # Delete from memory
        self.dal.deleteInMemoryDBSnapshot(db_snapshot_identifier)

    def describe(self, db_snapshot_identifier: str):
        '''Describe the details of the DB snapshot.'''
        if not self.dal.is_db_snapshot_exist(db_snapshot_identifier):
            # raise DBSnapshotNotFoundError('This DB instance identifier does not exist')
            pass
        
        describe_db_snapshot = self.dal.describeDBSnapshot(db_snapshot_identifier)[0]
        describe_db_snapshot_dict = {
            'db_instance_identifier': describe_db_snapshot[0],
            'creation_date': describe_db_snapshot[1],
            'owner_alias': describe_db_snapshot[2],
            'status': describe_db_snapshot[3],
            'description': describe_db_snapshot[4],
            'progress': describe_db_snapshot[5],
            'url_snapshot': describe_db_snapshot[6],
            'object_name': describe_db_snapshot[7],
            'table_structure': describe_db_snapshot[8]
        }
        return {'DBSnapshot': describe_db_snapshot_dict}

        

 

    def modify(self, db_snapshot_identifier: str, owner_alias: Optional[str] = None, status: Optional[str] = None,
               description: Optional[str] = None, progress: Optional[str] = None):
        '''Modify an existing DBCluster.'''
        updates = {
            'db_snapshot_identifier': db_snapshot_identifier,
            'owner_alias': owner_alias,
            'status': status,
            'description': description,
            'progress': progress
        }
        
        # Validate specific parameters
        if description and not is_valid_db_snapshot_description(description):
            raise ValueError(f"Invalid description: {description}")
        if progress and not is_valid_progress(progress):
            raise ValueError(f"Invalid progress: {progress}")

        required_params = ['db_snapshot_identifier']
        all_params = ['owner_alias', 'status', 'description', 'progress']
        all_params.extend(required_params)

        filtered_updates = {key: value for key, value in updates.items() if key != 'db_snapshot_identifier'}
        set_clause = ', '.join([f"{key} = '{value}'" for key, value in filtered_updates.items()])     


        if owner_alias is not None:
            self.owner_alias = owner_alias
        if status is not None:
            self.status = status
        if description is not None:
            self.description = description
        if progress is not None:
            self.progress = progress

        self.dal.modifyDBSnapshot(db_snapshot_identifier, set_clause)

    def get(self, db_snapshot_indentifier):
        '''Get code object.'''
        # Return real-time object

        describe_result = self.describe(db_snapshot_indentifier)
        if describe_result:
            describe_result = describe_result['DBSnapshot']
            return SnapshotNaive(
                describe_result['db_snapshot_identifier'],
                describe_result['db_instance_identifier'],
                describe_result['creation_date'],
                describe_result['owner_alias'],
                describe_result['status'],
                describe_result['description'],
                describe_result['progress'],
                describe_result['url_snapshot']
            )

        return None
    


     
               
        
