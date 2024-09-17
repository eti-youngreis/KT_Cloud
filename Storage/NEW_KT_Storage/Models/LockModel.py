from datetime import datetime
from typing import Dict
import sys
import json

sys.path.append('C:/Users/תמר מליק/bootcamp/project/KT_Cloud/NEW_KT_Storage')
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager

class LockModel:

    def __init__(self, bucket_key: str, object_key: str, retain_until: datetime, lock_mode: str): 
        self.lock_id = '.'.join([bucket_key, object_key])
        self.bucket_key = bucket_key
        self.object_key = object_key
        self.retain_until = retain_until
        self.lock_mode = lock_mode
        # attributes for memory management in database
        # self.pk_column = 'lock_id'
        self.pk_column = 'object_id'
        self.pk_value = self.lock_id
#             table_columns = "LockId TEXT PRIMARY KEY", "BucketKey TEXT", "ObjectKey TEXT","RetainUntil" ,"LockMode TEXT", "Unit TEXT"

    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''
        return ObjectManager(db_file=f"D:\\s3_project\\tables\\Locks.db", type="Lock")\
            .convert_object_attributes_to_dictionary(
            object_id = self.lock_id,
            bucket_key = self.bucket_key,
            object_key=self.object_key,
            retain_until=self.retain_until,
            lock_mode=self.lock_mode,
            pk_column=self.pk_column,
        )
        
    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\'' for v in data_dict.values()) + ')'
        return values
    
    def to_string(self):
        # Convert the dictionary to a JSON string
        return json.dumps(self.to_dict(), default=self.default_converter, indent=4)

    def default_converter(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO 8601 string
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
