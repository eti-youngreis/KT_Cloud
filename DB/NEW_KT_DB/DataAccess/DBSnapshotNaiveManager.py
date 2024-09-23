import json
import sqlite3
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..")))
from typing import Dict, Any
from DB.NEW_KT_DB.Validation.DBSnapshotNaiveValidations import is_valid_db_instance_id
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.DBSnapshotNaiveModel import SnapshotNaive

class DBSnapshotNaiveManager:
    
    def __init__(self, object_manager: ObjectManager):
        self.object_manager = object_manager
        # Create the management table for DBSnapshot using its object name and table structure
        self.object_manager.create_management_table(SnapshotNaive.object_name, SnapshotNaive.table_structure)

    def createInMemoryDBSnapshot(self, db_instance_identifier: SnapshotNaive):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_instance_identifier):
            raise ValueError(f"Invalid db_instance_identifier: {db_instance_identifier}")

        self.object_manager.save_in_memory(SnapshotNaive.object_name, db_instance_identifier.to_sql())

    def deleteInMemoryDBSnapshot(self, db_snapshot_identifier: str):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_snapshot_identifier, 15):
            raise ValueError(f"Invalid db_instance_identifier: {db_snapshot_identifier}")

        self.object_manager.delete_from_memory(
            pk_column = SnapshotNaive.pk_column,
            pk_value = db_snapshot_identifier,
            object_name = SnapshotNaive.object_name
        )

    def describeDBSnapshot(self, db_snapshot_identifier: str):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_snapshot_identifier, 15):
            raise ValueError(f"Invalid db_instance_identifier: {db_snapshot_identifier}")

        self.object_manager.get_from_memory(
            criteria=f"{SnapshotNaive.pk_column} = '{db_snapshot_identifier}'", 
            object_name=SnapshotNaive.object_name, 
            columns='*'
        )

    def modifyDBSnapshot(self, db_snapshot_identifier: str, updates: str):
        # Validate db_instance_identifier
        if not is_valid_db_instance_id(db_snapshot_identifier, 15):
            raise ValueError(f"Invalid db_instance_identifier: {db_snapshot_identifier}")
        
        # Assuming new_data contains fields to update, you might want to validate these fields as well
        # For example:
        # if 'description' in new_data and not is_valid_db_snapshot_description(new_data['description']):
        #     raise ValueError(f"Invalid description: {new_data['description']}")

        self.object_manager.update_in_memory(
            criteria=f"{SnapshotNaive.pk_column} = '{db_snapshot_identifier}'", 
            object_name=SnapshotNaive.object_name, 
            updates=updates
        )

    def is_db_snapshot_exist(self, db_snapshot_identifier: int) -> bool:
        return bool(self.object_manager.db_manager.is_object_exist(
            self.object_manager._convert_object_name_to_management_table_name(SnapshotNaive.object_name), 
            criteria=f"{SnapshotNaive.pk_column} = '{db_snapshot_identifier}'"
        ))
