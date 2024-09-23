import os
import sys
import json
from datetime import datetime
from typing import Dict, Optional
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..")))
from DB.NEW_KT_DB.DataAccess import ObjectManager
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager

from DB.NEW_KT_DB.Validation.DBSnapshotNaiveValidations import is_valid_db_instance_id, is_valid_db_snapshot_description, is_valid_progress, is_valid_date, is_valid_url_parameter

class SnapshotNaive:
    BASE_PATH = "db_snapshot"
    object_name = "db_snapshot_naive"
    pk_column = "db_snapshot_id"
    pk_column_data_type = 'TEXT'
    table_structure = f'''
        db_instance_identifier TEXT PRIMARY KEY,
        metadata TEXT NOT NULL
        '''
    def __init__(self, db_snapshot_identifier, db_instance_identifier: str, creation_date: datetime, owner_alias: str, status: str,
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
        self.db_snapshot_identifier = db_snapshot_identifier
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
            db_snapshot_identifier = self.db_snapshot_identifier,
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

    def to_sql(self):
        # Convert the model snapshot to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                                 for v in data_dict.values()) + ')'
        return values


