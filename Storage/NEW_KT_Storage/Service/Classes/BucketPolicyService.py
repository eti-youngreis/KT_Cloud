import os
import sys

from Models.BucketPolicyModel import BucketPolicy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import Dict, Optional
from Abc.STO import STO
# from Validation import Validation
from DataAccess.BucketPolicyManager import BucketPolicyManager

class ParamValidationFault(Exception):
    pass
class IsExistPermissionFault(Exception):
    pass


class BucketPolicyService(STO):
    def __init__(self, dal: BucketPolicyManager):
        self.dal = dal


    def create(self,bucket_name, permissions=[], allow_versions=True) -> bool:
        """
        Create a new bucket policy.
        :param bucket_policy: The bucket policy to create.
        :return: True if the policy was successfully created, False otherwise.
        """
        if not bucket_name:
            raise ParamValidationFault("bucket name is missing")

        bucket_policy = BucketPolicy(bucket_name, permissions=permissions, allow_versions=allow_versions)
        
        # save in memory
        self.dal.createInMemoryBucketPolicy(bucket_policy)
        # create an physical object
        self.dal.createPhysicalObject(bucket_policy)

    def delete(self, bucket_name: str, permission):

        # delete physical object
        self.dal.deletePhysicalObject(bucket_name, permission)
        # delete from memory
        self.dal.deleteInMemoryBucketPolicy(bucket_name, permission)

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

    def modify(self, bucket_name: str, new_permissions: dict, allow_versions=None ) -> bool:
        """
        Update an existing bucket policy.
        :param bucket_policy: The bucket policy to update.
        :return: True if the policy was successfully updated, False otherwise.
        """

        policy = self.dal.get(bucket_name)
        
        if new_permissions in policy['permissions']:
            raise IsExistPermissionFault("the permission already exist")
        
        policy['permissions'].append(new_permissions)
        if allow_versions != None:
            policy['allow_versions']=allow_versions
        self.dal.putBucketPolicy(policy)


