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
class IsNotExistFault(Exception):
    pass


class BucketPolicyService(STO):
    def __init__(self, dal: BucketPolicyManager):
        self.dal = dal


    def create(self,bucket_name, permissions=[], allow_versions=True) -> bool:

        if not bucket_name:
            raise ParamValidationFault("bucket name is missing")
        
        if permissions != []:
            permissions = [permissions]

        bucket_policy = BucketPolicy(bucket_name, permissions=permissions, allow_versions=allow_versions)
        
        # save in memory
        self.dal.createInMemoryBucketPolicy(bucket_policy)
        # create an physical object
        self.dal.createPhysicalObject(bucket_policy)
        return self.describe(bucket_name)

    def delete(self, bucket_name: str):

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

    def modify(self, bucket_name: str, update_permissions: list = [], allow_versions=None ) -> bool:
        """
        Update an existing bucket policy.
        :param bucket_policy: The bucket policy to update.
        :return: True if the policy was successfully updated, False otherwise.
        """

        bucket_policy = self.dal.get(bucket_name)
        
        if update_permissions:
            bucket_policy['permissions'] = self._update_permissions(bucket_name, update_permissions)
            
        if allow_versions != None:
            bucket_policy['allow_versions']=allow_versions
        self.dal.putBucketPolicy(bucket_policy)
        
    def _update_permissions(self, bucket_name, update_permissions):
        
        bucket_policy = self.dal.get(bucket_name)

        policy_permissions = bucket_policy['permissions']
        
        for permission in update_permissions:
            if permission in policy_permissions:
                policy_permissions.remove(permission)
            else:
                policy_permissions.add(permission)
        
        return policy_permissions
        
        # if permission not in policy_permission:
        #     raise IsNotExistFault("The permission is not exist in the bucket policy")
        # if policy_permission:
            
        #     policy_permission.remove(permission)
        # for permission in new_permissions:
        #     if permission in policy['permissions']:
        #         raise IsExistPermissionFault(f'the permission: {permission} already exist')
            
            # policy['permissions'].append(permission)

