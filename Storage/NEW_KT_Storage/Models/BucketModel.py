import json
from datetime import datetime
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager

class Bucket:

    table_structure = ", ".join(["BucketId TEXT PRIMARY KEY", "Owner TEXT", "Region TEXT", "created_at DATETIME"])
    pk_column = "BucketId"

    def __init__(self, bucket_name: str, owner: str, region=None, create_at=None):
        self.bucket_name = bucket_name
        self.owner = owner
        self.pk_value = bucket_name
        self.create_at = create_at or datetime.now()
        self.region = region or "us-east-1"


    def to_dict(self):
        return ObjectManager.convert_object_attributes_to_dictionary(
            bucket_name=self.bucket_name,
            owner=self.owner,
            region=self.region,
            create_at=self.create_at
        )

    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values





