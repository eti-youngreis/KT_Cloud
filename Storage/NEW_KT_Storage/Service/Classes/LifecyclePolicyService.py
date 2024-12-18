import json
from typing import List
from Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager import LifecyclePolicyManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy
from Storage.NEW_KT_Storage.Validation.LifecyclePolicyValidations import validate_lifecycle_attributes


class LifecyclePolicyService:

    def __init__(self):
        self.object_name = "Lifecycle"
        self.manager = LifecyclePolicyManager(object_name=self.object_name)

    def create(self, policy_name: str, bucket_name: str, expiration_days: int,
               transitions_days_glacier: int, status: str, prefix: List[str]):
        """
        Create a new lifecycle policy.
        :param policy_name: Unique name of the policy.
        :param bucket_name: Name of the associated bucket.
        :param expiration_days: Number of days before expiration.
        :param transitions_days_glacier: Days before transitioning objects to Glacier.
        :param status: Policy status, default is 'Enabled'.
        :param prefix: List of prefixes for object filtering.
        """
        if self.get(policy_name):
            raise ValueError(f"Policy with name '{policy_name}' already exists")

        lifecycle_policy = LifecyclePolicy(
            policy_name=policy_name,
            bucket_name=bucket_name,
            expiration_days=expiration_days,
            transitions_days_glacier=transitions_days_glacier,
            status=status,
            prefix=prefix
        )
        self.manager.create(lifecycle_policy)

    def get(self, policy_name: str):
        """
        Retrieve a lifecycle policy by its name.
        :param policy_name: The name of the policy.
        :return: The lifecycle policy object, if found.
        """
        return self.manager.get(policy_name=policy_name)

    def delete(self, policy_name: str):
        """
        Delete a lifecycle policy by its name.
        :param policy_name: The name of the policy to delete.
        """
        self.manager.delete(policy_name=policy_name)

    def modify(self, policy_name: str, bucket_name: str = None,
               expiration_days: int = None, transitions_days_glacier: int = None,
               status: str = None, prefix: List[str] = None):
        """
        Modify an existing lifecycle policy.
        :param policy_name: The name of the policy to modify.
        :param bucket_name: New bucket name, if changing.
        :param expiration_days: New expiration days value.
        :param transitions_days_glacier: New transition days to Glacier.
        :param status: New status for the policy.
        :param prefix: List of new prefixes for object filtering.
        """
        lifecycle = self.get(policy_name)
        if not lifecycle:
            raise ValueError(f"Policy '{policy_name}' not found")

        # Use original values if new ones are not provided
        lifecycle.bucket_name = bucket_name or lifecycle.bucket_name
        lifecycle.expiration_days = expiration_days if expiration_days is not None else lifecycle.expiration_days
        lifecycle.transitions_days_glacier = transitions_days_glacier if transitions_days_glacier is not None else lifecycle.transitions_days_glacier
        lifecycle.status = status or lifecycle.status
        lifecycle.prefix = prefix or lifecycle.prefix

        validate_lifecycle_attributes(
            policy_name=policy_name,
            expiration_days=lifecycle.expiration_days,
            transitions_days_glacier=lifecycle.transitions_days_glacier,
            status=lifecycle.status
        )

        self.manager.update(policy_name, lifecycle)

    def describe(self, policy_name: str):
        """
        Describe the details of a lifecycle policy.
        :param policy_name: The name of the policy to describe.
        :return: Details of the specified lifecycle policy.
        """
        lifecycle = self.get(policy_name=policy_name)
        if lifecycle:
            return self.manager.describe(lifecycle)

