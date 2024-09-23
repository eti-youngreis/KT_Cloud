import json
import sqlite3
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..")))
from typing import Dict, Any
from DB.NEW_KT_DB.Validation.DBSnapshotValidationsNaive import is_valid_db_instance_id
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.DBSnapshotModelNaive import SnapshotNaive

class DBSnapshotManagerNaive:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.db_file = db_file
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(SnapshotNaive.object_name, SnapshotNaive.table_structure)

    def createInMemoryDBSnapshot(self, db_instance_identifier: str):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_instance_identifier):
            raise ValueError(f"Invalid db_instance_identifier: {db_instance_identifier}")

        self.object_manager.save_in_memory()

    def deleteInMemoryDBSnapshot(self, db_instance_identifier: str):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_instance_identifier, 15):
            raise ValueError(f"Invalid db_instance_identifier: {db_instance_identifier}")

        self.object_manager.delete_from_memory()

    def describeDBSnapshot(self, db_instance_identifier: str):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_instance_identifier, 15):
            raise ValueError(f"Invalid db_instance_identifier: {db_instance_identifier}")

        self.object_manager.get_from_memory()

    def modifyDBSnapshot(self, db_instance_identifier: str, new_data: Dict[str, Any]):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_instance_identifier, 15):
            raise ValueError(f"Invalid db_instance_identifier: {db_instance_identifier}")
        
        # Assuming new_data contains fields to update, you might want to validate these fields as well
        # For example:
        # if 'description' in new_data and not is_valid_db_snapshot_description(new_data['description']):
        #     raise ValueError(f"Invalid description: {new_data['description']}")

        self.object_manager.update_in_memory()
