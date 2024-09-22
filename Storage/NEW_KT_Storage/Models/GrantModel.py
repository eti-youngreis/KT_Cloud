from datetime import datetime
from typing import Dict
from DataAccess import ObjectManager
import uuid
class GrantObject:

    def __init__(self, **kwargs): 

        # add relevant attributes in this syntax:
        # self.bucket_object_identifier = kwargs['bucket_object_identifier']
        # self.engine = kwargs['engine']

        self.grant_id = str(uuid.uuid1())
        self.name=kwargs['name'] 
        self.user=kwargs['user'] 
        self.pk_column = "grant_id"
        self.pk_value =kwargs['name'] 
        self.permission=kwargs['permission'] 
        self.objectManager = ObjectManager(db_file="D:\\s3_project\\tables\\Buckets.db")

        # self.permissions =kwargs['permissions'] or []
        # self.default_acl=kwargs['default_acl'] 
        
        # self.version_id=kwargs['version_id']
       



        # attributes for memory management in database
        # self.pk_column = kwargs.get('pk_column', 'ClusterID')
        # self.pk_value = kwargs.get('pk_value', None)


    def to_dict(self) -> Dict:
        '''Retrieve the data of the DB cluster as a dictionary.'''

        # send relevant attributes in this syntax:
        return ObjectManager.convert_object_attributes_to_dictionary(
            owner=self.owner
            permission= self.Permission,
            name= self.name,
            version_id=self.version_id,
            pk_column=self.pk_column,
            pk_value=self.pk_value
        )


    # כאן נוכל לקרוא לפונקציה כללית במנהל האובייקטים
    # def to_dict_with_manager(self, **kwargs):
    #     return NEW_KT_Storage.ObjectManager.to_dict(**kwargs)