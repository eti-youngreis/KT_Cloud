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
    """Exception raised when a permission does not exist in the policy."""
    pass


class BucketPolicyService(STO):
    """
    Service class to handle operations related to bucket policies.

    This class provides methods for creating, retrieving, updating, and deleting
    bucket policies using the underlying `BucketPolicyManager` for storage.

    Attributes:
    -----------
    dal : BucketPolicyManager
        Data access layer (DAL) to interact with the policy storage.

    """

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

        Raises:
        -------
        ParamValidationFault
            If the bucket name is not provided.

        Returns:
        --------
        bool
            True if the policy was created successfully, False otherwise.
        """
        if bucket_name is None:
            raise ParamValidationFault("bucket name is missing")
        
        if permissions != []:
            permissions = [permissions]

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
        return self.dal.describeBucketPolicy(bucket_name)

    def modify(self, bucket_name: str, update_permissions: list = [], allow_versions=None) -> bool:
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
        bucket_policy = self.dal.getBucketPolicy(bucket_name)
        
        if update_permissions:
            bucket_policy['permissions'] = self._update_permissions(bucket_name, update_permissions)
            
        if allow_versions is not None:
            bucket_policy['allow_versions'] = allow_versions
        
        self.dal.putBucketPolicy(bucket_policy)
        return True
        
    def _update_permissions(self, bucket_name, update_permissions):
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
        bucket_policy = self.dal.getBucketPolicy(bucket_name)
        policy_permissions = bucket_policy['permissions']
        
        for permission in update_permissions:
            if permission in policy_permissions:
                policy_permissions.remove(permission)
            else:
                policy_permissions.append(permission)
        
        return policy_permissions
