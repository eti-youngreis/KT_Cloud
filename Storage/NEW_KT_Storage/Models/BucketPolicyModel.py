from datetime import datetime
from typing import Dict, Optional
from DataAccess import ObjectManager

class BucketPolicy:

    def __init__(self, policy_id, bucket_name: str, permissions: dict):
        # , pk_column, pk_value
        self.policy_id = policy_id
        self.bucket_name = bucket_name
        self.permissions = permissions
    
        # attributes for memory management in database
        # self.pk_column = pk_column
        # self.pk_value = pk_value


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        return ObjectManager.convert_object_attributes_to_dictionary(
            
            bucket_name=self.bucket_name,
            # policy_content=self.policy_content,
            policy_id=self.policy_id,
            # version=self.version,
            # tags=self.tags if self.tags else {},
            # available=self.available,
            permissions = self.permissions,
            
            pk_column=self.pk_column,
            pk_value=self.pk_value
        )
    # def to_sql(self):
    #     # Convert the model instance to a dictionary
    #     data_dict = self.to_dict()
    #     values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
    #                        for v in data_dict.values()) + ')'
    #     return values