import os
import sys

from Models.BucketPolicyModel import BucketPolicy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import Dict, Optional
from Abc.STO import STO
# from Validation import Validation
from DataAccess.BucketPolicyManager import BucketPolicyManager

class BucketPolicyService(STO):
    def __init__(self, dal: BucketPolicyManager):
        self.dal = dal
    
    def validate_policy(self, bucket_policy: BucketPolicy) -> bool:
        """
        Validate the bucket policy.
        :param bucket_policy: The bucket policy to validate.
        :return: True if the policy is valid, False otherwise.
        """
        # Example validation logic (customize as needed)
        if 'policy_id' not in bucket_policy:
            return False
        if 'bucket_name' not in bucket_policy or not bucket_policy['bucket_name']:
            return False
        if 'permissions' not in bucket_policy or not bucket_policy['permissions']:
            return False
        return True

    def create(self, bucket_policy: BucketPolicy) -> bool:
        """
        Create a new bucket policy.
        :param bucket_policy: The bucket policy to create.
        :return: True if the policy was successfully created, False otherwise.
        """
        if not self.validate_policy(bucket_policy):
            return False

        if self.dal.get(bucket_policy['bucket_name']) is not None:
            print("Policy already exists. Use modify() to update.")
            return False
        
        # save in memory
        self.dal.createInMemoryBucketPolicy(bucket_policy)
        # create an physical object
        self.dal.createPhysicalObject(bucket_policy)

    def delete(self, bucket_name: str):
        '''Delete an existing BucketObject.'''
        # assign None to code object
        # delete physical object
        self.dal.deletePhysicalObject(bucket_name)
        # delete from memory
        self.dal.deleteInMemoryBucketPolicy(bucket_name)

    def get(self, bucket_name):
        '''get code object.'''
        # return real time object
        return self.dal.get(bucket_name)
    
    def put(self, **updates):
        '''Modify an existing BucketObject.'''
        # update object in code
        # modify physical object
        # update object in memory
        pass
    
    def describe(self, bucket_name):
        '''Describe the details of BucketObject.'''
        return self.dal.describeBucketPolicy(bucket_name)

    def modify(self, bucket_name: str, new_permissions: dict) -> bool:
        """
        Update an existing bucket policy.
        :param bucket_policy: The bucket policy to update.
        :return: True if the policy was successfully updated, False otherwise.
        """
        # if not self.validate_policy(bucket_policy):
        #     return False

        existing_policy = self.dal.get(bucket_name)
        if existing_policy is None:
            print("Policy does not exist. Use create() to add.")
            return False

        existing_policy['permissions'] = new_permissions
        self.dal.putBucketPolicy(existing_policy)


