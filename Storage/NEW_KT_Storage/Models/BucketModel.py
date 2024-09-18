import json

# from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject
# from typing import List,Dict
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager

class Bucket:
    def __init__(self, bucket_name: str, owner: str):
        self.bucket_name = bucket_name
        self.owner = owner
        self.pk_column = "object_id"
        self.pk_value = bucket_name
        self.objectManager = ObjectManager(db_file="D:\\s3_project\\tables\\Buckets.db")


    def to_dict(self):
        return self.objectManager.convert_object_attributes_to_dictionary(
            pk_value=self.pk_value,
            owner=self.owner,
        )

    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values





