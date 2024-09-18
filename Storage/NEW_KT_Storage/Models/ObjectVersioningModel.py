from datetime import datetime
from typing import Dict
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
import json

class VersionObject:
    def __init__(self,bucket_name: str, object_key: object, content = " ", version_id: object = None, is_latest: object = False, last_modified: object = None, etag: object = None, size: object = None,
                 storage_class: object = "STANDARD", owner: object = None, checksum_algorithm: object = None, delete_marker: object = False,
                 replication_status: object = None, object_lock_mode: object = None, object_lock_retain_until_date: object = None,
                 pk_column: str = None, pk_value: str = None) -> object:
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.version_id = version_id
        self.content = content
        self.is_latest = is_latest
        self.last_modified = last_modified if last_modified else datetime.now()
        self.etag = etag
        self.size = size
        self.storage_class = storage_class
        self.owner = owner
        self.checksum_algorithm = checksum_algorithm
        self.delete_marker = delete_marker
        self.replication_status = replication_status
        self.object_lock_mode = object_lock_mode
        self.object_lock_retain_until_date = object_lock_retain_until_date

        # attributes for memory  management in database
        self.pk_column = pk_column
        self.pk_value = pk_value

    def to_dict(self) -> Dict:
        '''Retrieve the data of the version object as a dictionary.'''
        return ObjectManager.convert_object_attributes_to_dictionary(
            pk = self.bucket_name + self.object_key + self.version_id,
            bucket_name = self.bucket_name,
            content = self.content,
            version_id=self.version_id,
            object_key=self.object_key,
            is_latest=self.is_latest,
            last_modified=self.last_modified,
            size=self.size,
            owner=self.owner,
        )



    def to_sql(self):

        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values


