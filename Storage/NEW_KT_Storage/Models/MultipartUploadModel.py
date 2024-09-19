import uuid
from typing import Optional
from datetime import datetime
import json
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DataAccess.ObjectManager import ObjectManager

class MultipartUploadModel:
    TABLE_STRUCTURE = "object_id TEXT PRIMARY KEY,object_name TEXT NOT NULL, bucket_name TEXT NOT NULL,parts TEXT NOT NULL"

    def __init__(self,object_key: str,bucket_name:str,upload_id:str,parts=[]):
        """Create a unique upload model with bucket and object key"""
        self.upload_id =upload_id
        self.bucket_name=bucket_name
        self.object_key = object_key  # Object key (usually file name)
        self.parts = parts
        self.pk_column = "object_id"
        self.pk_value = self.upload_id
        self.objectManager = ObjectManager(db_file="my_db.db")


    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values
    
    
    def to_dict(self):
        return ObjectManager.convert_object_attributes_to_dictionary(
            pk_value=self.pk_value,
            object_key=self.object_key,
            bucket_name=self.bucket_name,
            parts=self.parts

        )