import uuid
from typing import Optional
from datetime import datetime
import json


class MultipartUploadModel:
    def __init__(self, bucket_name: str, object_key: str):
        """Create a unique upload model with bucket and object key"""
        self.upload_id = str(uuid.uuid4())  # Create a unique identifier for the upload
        self.bucket_name = bucket_name  # Bucket name
        self.object_key = object_key  # Object key (usually file name)
        self.parts = "[] " # List of parts

    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values

    def to_dict(self):
        return {
            'bucket_name': self.bucket_name,
            'object_key': self.object_key,
            'upload_id': self.upload_id,
            'parts': self.parts
        }
    # def __init__(self, bucket_name: str, object_key: str):
    #     """יצירת מודל העלאה עם bucket ומפתח אובייקט ייחודי"""
    #     self.bucket_name = bucket_name  # שם ה-bucket
    #     self.object_key = object_key  # מפתח האובייקט (לרוב שם קובץ)
    #     self.upload_id = str(uuid.uuid4())  # יצירת מזהה ייחודי עבור ההעלאה
    #     self.parts = []  # רשימת חלקים

    # def to_dict(self):
    #     return {
    #         'bucket_name': self.bucket_name,
    #         'object_key': self.object_key,
    #         'upload_id': self.upload_id,
    #         'parts': self.parts
    #     }