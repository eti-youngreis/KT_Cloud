from typing import Dict, Optional
from Models import BucketPolicyModel
from Abc import STO
from Validation import Validation
from DataAccess import BucketPolicyManager

class BucketPolicyService(STO):
    def __init__(self, dal: BucketPolicyManager):
        self.dal = dal
    
    
    # validations here
    

    def create(self, bucket_name: str, policy_content: str, policy_name: str, version: str = "2012-10-17", tags: Optional[Dict] = None, available: bool = True):
        '''Create a new BucketPolicy.'''
        bucketPolicy = BucketPolicyModel(bucket_name, policy_name, version, tags, available)
        # create physical object as described in task
        self.dal.ObjectManager.StorageManager.create_file(file_path='{bucket_name}.{policy_name}', content=policy_content)
        # save in memory using BucketObjectManager.createInMemoryBucketPolicy() function
        self.dal.createInMemoryBucketPolicy()

    def delete(self):
        '''Delete an existing BucketObject.'''
        # assign None to code object
        # delete physical object
        self.dal.ObjectManager.StorageManager.delete_file()
        # delete from memory using BucketObjectManager.deleteInMemoryBucketObject() function- send criteria using self attributes
        self.dal.deleteInMemoryBucketPolicy()


    def describe(self):
        '''Describe the details of BucketObject.'''
        # use BucketObjectManager.describeBucketObject() function
        pass


    def put(self, **updates):
        '''Modify an existing BucketObject.'''
        # update object in code
        # modify physical object
        # update object in memory using BucketObjectManager.modifyInMemoryBucketObject() function- send criteria using self attributes
        pass


    def get(self):
        '''get code object.'''
        # return real time object
        pass
