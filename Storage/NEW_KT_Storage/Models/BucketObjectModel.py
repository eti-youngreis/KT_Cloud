from datetime import datetime
from typing import Dict
from DataAccess import ObjectManager

class BucketObject:

    def __init__(self, **kwargs): 

        # add relevant attributes in this syntax:
        # self.bucket_object_identifier = kwargs['bucket_object_identifier']
        # self.engine = kwargs['engine']

        # attributes for memory management in database
        self.pk_column = kwargs.get('pk_column', 'ClusterID')
        self.pk_value = kwargs.get('pk_value', None)


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        # send relevant attributes in this syntax:
        return ObjectManager.convert_object_attributes_to_dictionary(
            # bucket_object_identifier=self.bucket_object_identifier, 
            # engine=self.engine,
            pk_column=self.pk_column,
            pk_value=self.pk_value
        )