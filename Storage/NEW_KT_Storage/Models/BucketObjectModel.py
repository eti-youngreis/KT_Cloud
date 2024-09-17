from datetime import datetime
from typing import Dict

from uuid import uuid1

from pandas.io import json

from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager


class BucketObject:

    def __init__(self, **kwargs):
        # attributes related to S3
        self.bucket_name = kwargs.get('bucket_name')
        # self.bucket_id=kwargs.get('bucket_id')
        self.object_key = kwargs.get('object_key')

        # versioning attributes
        self.versions = kwargs.get('versions', None)

        # attributes for memory management in database
        self.pk_column = kwargs.get('pk_column', 'object_id')
        self.pk_value = self.bucket_name+self.object_key

        self.encryption_id = kwargs.get('encryption_id',None)
        self.lock_id = kwargs.get('lock_id',None)

        # self.created_at = kwargs.get('created_at', datetime.now())
        # self.updated_at = kwargs.get('updated_at', datetime.now())
        # self.content_type = kwargs.get('content_type', 'application/octet-stream')
        # self.body = kwargs.get('body', None)
        # self.metadata = kwargs.get('metadata', {})
        # self.acl = kwargs.get('acl', 'private')
        # self.storage_class = kwargs.get('storage_class', 'STANDARD')
        # self.encryption = kwargs.get('encryption', None)

    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''
        return ObjectManager("C:\\Users\\user1\\Desktop\\server\\object.db","Objects").convert_object_attributes_to_dictionary(
            pk_value=self.pk_value,
            bucket_name=self.bucket_name,
            object_key=self.object_key,
            encryption_id=self.encryption_id,
            lock_id=self.lock_id,
            # versions=[v.to_dict() for v in self.versions],

            # created_at=self.created_at,
            # updated_at=self.updated_at
        )

    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(
            f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v,
                                                                                                               str) else f'\'{str(v)}\''
            for v in data_dict.values()) + ')'
        return values
