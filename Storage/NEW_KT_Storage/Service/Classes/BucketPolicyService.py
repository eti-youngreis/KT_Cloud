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

class IsExistactionnFault(Exception):
    """Exception raised when a actionn already exists in the policy."""
    pass

class IsNotExistFault(Exception):
    """Exception raised when a bucket_name does not exist in the policy."""
    pass
class IsNotExistactionnFault(Exception):
    """Exception raised when a actionn does not exist in the policy."""
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

    def create(self, bucket_name, actions=[], allow_versions=True) -> bool:
        """
        Create a new bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket for which the policy is being created.
        actions : list, optional
            A list of actions for the bucket (default is an empty list).
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
        
        # Ensure actions is a list and contains valid actionn strings
        if not isinstance(actions, list):
            raise ParamValidationFault("actions must be a list.")
        
        valid_actions = {'READ', 'WRITE', 'DELETE', 'CREATE'}  # Example actions
        for actionn in actions:
            if actionn not in valid_actions:
                raise ParamValidationFault(f"Invalid actionn: {actionn}")

        # Ensure allow_versions is boolean
        if not isinstance(allow_versions, bool):
            raise ParamValidationFault("allow_versions must be a boolean value.")

        bucket_policy = BucketPolicy(bucket_name, actions=actions, allow_versions=allow_versions)
        
        # Save in-memory
        self.dal.createInMemoryBucketPolicy(bucket_policy)
        # Create a physical object
        self.dal.createPolicy(bucket_policy)
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
        self.dal.deletePolicy(bucket_name)
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

    def modify(self, bucket_name: str, update_actions: list = [], allow_versions=None, action = None) -> bool:
        """
        Update an existing bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is being updated.
        update_actions : list, optional
            The new actions to add or remove (default is an empty list).
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
            if not update_actions:
                raise ParamValidationFault("actions must be inilaize")

                    
            bucket_policy['actions'] = self._update_actions(bucket_name, action, update_actions)
            
        if allow_versions is not None:
            # Ensure allow_versions is boolean
            if not isinstance(allow_versions, bool):
                raise ParamValidationFault("allow_versions must be a boolean value.")
            bucket_policy['allow_versions'] = allow_versions
        
        self.dal.putBucketPolicy(bucket_policy)
        return True
        
    def _update_actions(self, bucket_name, action, update_actions):
        """
        Helper method to update the actions of a bucket policy.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose actions are being updated.
        update_actions : list
            The list of actions to be added or removed.

        Returns:
        --------
        list
            The updated list of actions.
        """
        if not self.dal.getBucketPolicy(bucket_name):
            raise IsNotExistFault(f"Bucket policy for '{bucket_name}' does not exist.")
        
        # Ensure actions is a list and contains valid actionn strings
        if not isinstance(update_actions, list):
            raise ParamValidationFault("actions must be a list.")
        
        bucket_policy = self.dal.getBucketPolicy(bucket_name)
        policy_actions = bucket_policy['actions']
        
        for actionn in update_actions:
            valid_actions = {'READ', 'WRITE', 'DELETE', 'CREATE'}
            if actionn not in valid_actions:
                raise ParamValidationFault(f"actionn {actionn} already exists in the bucket policy.")
            
        if action == "add":
            policy_actions = self._add_actions(policy_actions, update_actions)
        else:
            policy_actions = self._delete_actions(policy_actions, update_actions)
            
    
    def _add_actions(self, policy_actions, actions = []):

        for actionn in actions:
            if actionn in policy_actions:
                raise IsExistactionnFault("actionn already exist in the bucket")
            policy_actions.append(actionn)
            
        return policy_actions
            
    def _delete_actions(self, policy_actions, actions = []):
        
        print("print" ,policy_actions)
        for actionn in actions:
            if actionn not in policy_actions:
                raise IsNotExistactionnFault("actionn not exist in the bucket policy")
            policy_actions.remove(actionn)
        
        return policy_actions
        
        