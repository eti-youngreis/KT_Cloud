import os
import sys
from Models.BucketPolicyModel import BucketPolicy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from typing import Dict, Optional
from Abc.STO import STO
# from Validation import Validation
from DataAccess.BucketPolicyManager import BucketPolicyManager

class ParamValidationFault(Exception):
    """Exception raised when a required parameter is missing."""
    pass

class IsExistPermissionFault(Exception):
    """Exception raised when a permission already exists in the policy."""
    pass

class IsNotExistFault(Exception):
    """Exception raised when a bucket_name does not exist in the policy."""
    pass
class IsNotExistPermissionFault(Exception):
    """Exception raised when a permission does not exist in the policy."""
    pass

class BucketPolicyService(STO):

    def __init__(self, dal: BucketPolicyManager):
        """
        Initializes the BucketPolicyService with a data access layer (DAL).

        Parameters:
        -----------
        dal : BucketPolicyManager
            The data access layer responsible for managing the storage of bucket policies.
        """
        self.dal = dal

    def create(self, bucket_name, permissions=[], allow_versions=True) -> bool:
        """
        Create a new bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket for which the policy is being created.
        permissions : list, optional
            A list of permissions for the bucket (default is an empty list).
        allow_versions : bool, optional
            Whether or not to allow versioning (default is True).

        Returns:
        --------
        bool
            True if the policy was created successfully, False otherwise.
        """
        # Ensure bucket_name is provided
        if not bucket_name or not isinstance(bucket_name, str):
            raise ParamValidationFault("Bucket name must be a valid non-empty string.")
        
        # Ensure permissions is a list and contains valid permission strings
        if not isinstance(permissions, list):
            raise ParamValidationFault("Permissions must be a list.")
        
        valid_permissions = {'READ', 'WRITE', 'DELETE', 'CREATE'}  # Example permissions
        for permission in permissions:
            if permission not in valid_permissions:
                raise ParamValidationFault(f"Invalid permission: {permission}")

        # Ensure allow_versions is boolean
        if not isinstance(allow_versions, bool):
            raise ParamValidationFault("allow_versions must be a boolean value.")

        bucket_policy = BucketPolicy(bucket_name, permissions=permissions, allow_versions=allow_versions)
        
        # Save in-memory
        self.dal.createInMemoryBucketPolicy(bucket_policy)
        # Create a physical object
        self.dal.createPhysicalPolicy(bucket_policy)
        return self.describe(bucket_name)

    def delete(self, bucket_name: str):
        """
        Delete an existing bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is to be deleted.
        """
        # Check if the bucket exists before attempting to delete
        if not self.dal.getBucketPolicy(bucket_name):
            raise IsNotExistFault(f"Bucket policy for '{bucket_name}' does not exist.")
        # Delete physical object
        self.dal.deletePhysicalPolicy(bucket_name)
        # Delete from memory
        self.dal.deleteInMemoryBucketPolicy(bucket_name)

    def get(self, bucket_name):
        """
        Retrieve a bucket policy by bucket name.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is being retrieved.

        Returns:
        --------
        Dict
            The bucket policy if found, otherwise None.
        """
        # Check if the bucket exists before attempting to delete
        if not self.dal.getBucketPolicy(bucket_name):
            raise IsNotExistFault(f"Bucket policy for '{bucket_name}' does not exist.")
        return self.dal.getBucketPolicy(bucket_name)
    
    def put(self, **updates):
        """
        Modify an existing bucket policy.

        Parameters:
        -----------
        updates : dict
            The updates to apply to the bucket policy.
        """
        # Modify physical object
        # Update object in memory
        pass
    
    def describe(self, bucket_name):
        """
        Describe the details of a bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is being described.

        Returns:
        --------
        Dict
            The bucket policy details.
        """
        # Check if the bucket exists before attempting to delete
        if not self.dal.getBucketPolicy(bucket_name):
            raise IsNotExistFault(f"Bucket policy for '{bucket_name}' does not exist.")
        return self.dal.describeBucketPolicy(bucket_name)

    def modify(self, bucket_name: str, update_permissions: list = [], allow_versions=None, action = None) -> bool:
        """
        Update an existing bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is being updated.
        update_permissions : list, optional
            The new permissions to add or remove (default is an empty list).
        allow_versions : bool, optional
            Whether to enable or disable versioning (default is None).

        Returns:
        --------
        bool
            True if the policy was updated successfully, False otherwise.
        """
        
        #  Check if the bucket exists before attempting to delete
        if not self.dal.getBucketPolicy(bucket_name):
            raise IsNotExistFault(f"Bucket policy for '{bucket_name}' does not exist.")
        
        bucket_policy = self.dal.getBucketPolicy(bucket_name)
        
        if action != None:
            print("!!", action)
            if action not in ["add", "delete"]:
                raise ParamValidationFault("The action can be only add or delete")
            if not update_permissions:
                raise ParamValidationFault("permissions must be inilaize")

                    
            bucket_policy['permissions'] = self._update_permissions(bucket_name, action, update_permissions)
            
        if allow_versions is not None:
            # Ensure allow_versions is boolean
            if not isinstance(allow_versions, bool):
                raise ParamValidationFault("allow_versions must be a boolean value.")
            bucket_policy['allow_versions'] = allow_versions
        
        self.dal.putBucketPolicy(bucket_policy)
        return True
        
    def _update_permissions(self, bucket_name, action, update_permissions):
        """
        Helper method to update the permissions of a bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose permissions are being updated.
        update_permissions : list
            The list of permissions to be added or removed.

        Returns:
        --------
        list
            The updated list of permissions.
        """
        if not self.dal.getBucketPolicy(bucket_name):
            raise IsNotExistFault(f"Bucket policy for '{bucket_name}' does not exist.")
        
        # Ensure permissions is a list and contains valid permission strings
        if not isinstance(update_permissions, list):
            raise ParamValidationFault("Permissions must be a list.")
        
        bucket_policy = self.dal.getBucketPolicy(bucket_name)
        policy_permissions = bucket_policy['permissions']
        
        for permission in update_permissions:
            valid_permissions = {'READ', 'WRITE', 'DELETE', 'CREATE'}
            if permission not in valid_permissions:
                raise ParamValidationFault(f"Permission {permission} already exists in the bucket policy.")
            
        if action == "add":
            policy_permissions = self._add_permissions(policy_permissions, update_permissions)
        else:
            policy_permissions = self._delete_permissions(policy_permissions, update_permissions)
            
    
    def _add_permissions(self, policy_permissions, permissions = []):

        for permission in permissions:
            if permission in policy_permissions:
                raise IsExistPermissionFault("Permission already exist in the bucket")
            policy_permissions.append(permission)
            
        return policy_permissions
            
    def _delete_permissions(self, policy_permissions, permissions = []):
        
        print("print" ,policy_permissions)
        for permission in permissions:
            if permission not in policy_permissions:
                raise IsNotExistPermissionFault("Permission not exist in the bucket policy")
            policy_permissions.remove(permission)
        
        return policy_permissions
        
        