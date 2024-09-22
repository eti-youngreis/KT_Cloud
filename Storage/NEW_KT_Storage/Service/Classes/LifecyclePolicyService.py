import json
from typing import List

from Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager import LifecyclePolicyManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy
from Storage.NEW_KT_Storage.Validation.LifecyclePolicyValidations import validate_lifecycle_attributes


class LifecyclePolicyService:

    def __init__(self):
        self.object_name = "Lifecycle"
        self.manager = LifecyclePolicyManager(object_name="Lifecycle")

    def create(self, policy_name, expiration_days, transitions_days_GLACIER, status, prefix):
        if self.get(policy_name):
            raise ValueError(f"Policy with name '{policy_name}' already exists")

        lifecycle_policy = LifecyclePolicy(
            policy_name=policy_name,
            expiration_days=expiration_days,
            transitions_days_GLACIER=transitions_days_GLACIER,
            status=status,
            prefix=prefix
        )
        self.manager.create(lifecycle_policy)

    def get(self, policy_name):
        # Handle retrieval
        return self.manager.get(policy_name=policy_name)

    def delete(self, policy_name):
        # Handle physical deletion and memory management
        self.manager.delete(policy_name=policy_name)

    def modify(self, policy_name, expiration_days=None, transitions_days_GLACIER=None, status=None,
               prefix: List[str] = None):
        if policy_name:
            lifecycle = self.get(policy_name)
            if lifecycle:
                # Use the original values if the new values are None
                expiration_days = expiration_days if expiration_days is not None else lifecycle.expiration_days
                transitions_days_GLACIER = transitions_days_GLACIER if transitions_days_GLACIER is not None else lifecycle.transitions_days_GLACIER
                status = status if status is not None else lifecycle.status
                prefix = prefix if prefix is not None else lifecycle.prefix

                # Update the lifecycle object
                lifecycle.expiration_days = expiration_days
                lifecycle.transitions_days_GLACIER = transitions_days_GLACIER
                lifecycle.status = status
                lifecycle.prefix = prefix

                # Validate the updated attributes
                validate_lifecycle_attributes(policy_name=policy_name, expiration_days=expiration_days,
                                              transitions_days_GLACIER=transitions_days_GLACIER, status=status)

                # Update the policy
                self.manager.update(policy_name, lifecycle)
            else:
                print(f"Policy '{policy_name}' not found")
        else:
            print("Policy name is required")

    def describe(self, policy_id):
        # Describe the lifecycle policy and return the relevant details
        return self.manager.describe(policy_id)