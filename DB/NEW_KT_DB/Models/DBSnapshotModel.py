from datetime import datetime
from typing import Dict
from DataAccess import ObjectManager

class Snapshot:

    def __init__(self, db_name: str, creation_date: datetime, owner_alias: str, status: str,
                 description: str = None, progress: str = None, url_snapshot: str = None):
            self.db_name = db_name
            self.creation_date = creation_date
            self.owner_alias = owner_alias
            self.status = status
            self.description = description
            self.progress = progress
            self.url_snapshot = url_snapshot

       
    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB snapshot as a dictionary.'''
        
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_name = self.db_name,
            creation_date = self.creation_date,
            owner_alias = self.owner_alias,
            status = self.status,
            description = self.description,
            progress = self.progress,
            url_snapshot = self.url_snapshot
        )       
    




