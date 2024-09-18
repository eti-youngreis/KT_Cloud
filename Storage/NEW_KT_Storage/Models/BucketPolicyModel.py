from datetime import datetime
import json
from typing import Dict, Optional
import uuid
# from DataAccess import ObjectManager

class BucketPolicy:
    pk_column = 'policy_id'
    object_name = 'BucketPolicy'
    table_structure = '''
                policy_id VARCHAR(255) PRIMARY KEY NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                permissions TEXT NOT NULL,
                allow_versions BOOLEAN NOT NULL
            '''
    def __init__(self, bucket_name: str, permissions: list, allow_versions=True):
        # , pk_column, pk_value
        self.policy_id = self._generate_policy_id(bucket_name)
        self.bucket_name = bucket_name
        self.permissions = permissions
        self.allow_versions=allow_versions


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        # return ObjectManager.convert_object_attributes_to_dictionary(
            
        #     bucket_name=self.bucket_name,
        #     policy_id=self.policy_id,
        #     permissions = self.permissions,
        #     allow_versions=self.allow_versions
        # )
        return self.convert_object_attributes_to_dictionary(
            policy_id=self.policy_id,
            bucket_name=self.bucket_name,
            permissions = self.permissions,
            allow_versions=self.allow_versions)
    
    def convert_object_attributes_to_dictionary(self, **kwargs):
        dict = {}
        for key, value in kwargs.items():
            dict[key] = value

        return dict
        
    def _generate_policy_id(self, bucket_name:str):

        # Generate a unique identifier (UUID4)
        unique_id = str(uuid.uuid4())
        
        # Concatenate bucket_name with the unique identifier
        policy_id = f"{bucket_name}_{unique_id}"
        
        return policy_id
    
    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                                for v in data_dict.values()) + ')'
        return values