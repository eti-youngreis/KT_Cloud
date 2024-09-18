import json
from typing import List

from Storage.NEW_KT_Storage.DataAccess.LifecyclePolicyManager import LifecyclePolicyManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy


class LifecyclePolicyService:

    def __init__(self):
        self.manager = LifecyclePolicyManager()

    def create(self, policy_name, expiration_days, transitions_days_GLACIER, status,prefix):
        # Handle physical object, create RT code object, and save in memory
        lifecycle_policy = LifecyclePolicy(policy_name=policy_name, expiration_days=expiration_days, transitions_days_GLACIER=transitions_days_GLACIER, status=status,prefix=prefix)
        self.manager.create(lifecycle_policy)
        # Call necessary validation functions here

    def get(self, policy_name):
        # Handle retrieval
        return self.manager.get(policy_name=policy_name)

    def delete(self, policy_name):
        # Handle physical deletion and memory management
        self.manager.delete(policy_name=policy_name)

    def modify(self, policy_name, expiration_days=None, transitions_days_GLACIER=None, status=None,prefix:List[str]=[]):
        if policy_name:
            lifecycle = self.get(policy_name)
            if lifecycle:
                lifecycle.prefix = prefix
                lifecycle.expiration_days = expiration_days
                lifecycle.transitions_days_GLACIER = transitions_days_GLACIER
                lifecycle.status = status
                self.manager.update(policy_name, lifecycle)

    def describe(self, policy_id):
        # Describe the lifecycle policy and return the relevant details
        return self.manager.describe(policy_id)
