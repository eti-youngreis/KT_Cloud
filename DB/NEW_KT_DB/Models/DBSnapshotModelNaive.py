from datetime import datetime
from typing import Dict, Optional
from DataAccess import ObjectManager
from DB.NEW_KT_DB.Validation.DBSnapshotValidationsNaive import is_valid_db_instance_id, is_valid_db_snapshot_description, is_valid_progress, is_valid_date, is_valid_url_parameter

class Snapshot:

    def __init__(self, db_instance_identifier: str, creation_date: datetime, owner_alias: str, status: str,
                 description: Optional[str] = None, progress: Optional[str] = None, url_snapshot: Optional[str] = None):
        
        # Validate parameters
        if not is_valid_db_instance_id(db_instance_identifier):
            raise ValueError(f"Invalid db_instance_identifier: {db_instance_identifier}")
        if not is_valid_date(creation_date.strftime('%Y-%m-%d')):
            raise ValueError(f"Invalid creation_date: {creation_date}")
        if description:
            if not is_valid_db_snapshot_description(description):
                raise ValueError(f"Invalid description: {description}")
        if progress:
            if not is_valid_progress(progress):
                raise ValueError(f"Invalid progress: {progress}")
        if url_snapshot:
            if not is_valid_url_parameter(url_snapshot):
                raise ValueError(f"Invalid url_snapshot: {url_snapshot}")

        self.db_instance_identifier = db_instance_identifier
        self.creation_date = creation_date
        self.owner_alias = owner_alias
        self.status = status
        self.description = description
        self.progress = progress
        self.url_snapshot = url_snapshot
        self.object_name = "Snapshot"
        self.table_structure = {}

    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB snapshot as a dictionary.'''
        
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_instance_identifier = self.db_instance_identifier,
            creation_date = self.creation_date,
            owner_alias = self.owner_alias,
            status = self.status,
            description = self.description,
            progress = self.progress,
            url_snapshot = self.url_snapshot,
            object_name = self.object_name,
            table_structure = self.table_structure
        )



