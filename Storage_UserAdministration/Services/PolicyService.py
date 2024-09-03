from typing import List
from Storage_UserAdministration.DataAccess.policyManager import PolicyManager
from Storage_UserAdministration.Models.PermissionModel import Permission
from Storage_UserAdministration.Models.PolicyModel import PolicyModel
from Storage_UserAdministration.ML.LRUCache import LRUCache
class PolicyService:
    def __init__(self, dal: PolicyManager, cache_capacity: int = 100):
        self.dal = dal
        self.cache = LRUCache(cache_capacity)

    def create(self, policy_name: str, version: str, permissions: List[Permission] = None):
        """Create a new policy."""
        if not policy_name or not version:
            raise ValueError("Policy name and version are required.")
        if self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' already exists.")
        policy = PolicyModel(policy_name, version, permissions or [])
        self.dal.insert(policy.to_dict())
        self.cache.put(policy_name, policy)
        return policy
    def delete(self, policy_name: str):
        """Delete an existing policy."""
        if not self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        self.dal.delete(policy_name)
        self.cache.cache.pop(policy_name, None)

    def update(self, policy_name: str, version: str = None, permissions: List[Permission] = None):
        """Update an existing policy."""
        if not self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        current_data = self.dal.select(policy_name)
        updated_version = version or current_data.get('version')
        updated_permissions = permissions or [Permission.from_dict(p) for p in current_data.get('permissions', [])]
        updated_policy = PolicyModel(policy_name, updated_version, updated_permissions)
        self.dal.update(updated_policy)
        self.cache.put(policy_name, updated_policy)
        return policy_name, updated_policy

    def get(self, policy_name: str) -> PolicyModel:
        """Get a policy by name."""
        policy = self.cache.get(policy_name)
        if policy is None:
            data = self.dal.select(policy_name)
            if data is None:
                raise ValueError(f"Policy '{policy_name}' does not exist.")
            policy = PolicyModel(data['policyName'], data['version'], [Permission.from_dict(per) for per in data.get('permissions', [])])
            self.cache.put(policy_name, policy)
        return policy

    def list_policies(self) -> List[PolicyModel]:
        """List all policies."""
        policies = self.dal.select_all('Policy')
        return [PolicyModel(p['policyName'], p['version'], [Permission.from_dict(per) for per in p.get('permissions', [])]) for p in policies]

    def add_permission(self, policy_name: str, permission: Permission):
        """Add a permission to an existing policy."""
        if not self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        current_data = self.dal.select(policy_name)

        # Append the new permission to the existing permissions
        permissions = current_data.permissions
        permissions.append(permission)

        # Create a new updated policy object with the new list of permissions
        updated_policy = PolicyModel(policy_name, current_data.version, permissions)

        # Update the policy in the data access layer
        self.dal.update(updated_policy)
        self.cache.put(policy_name, updated_policy)

    def evaluate(self, policy_name: str, permissions: List[Permission]) -> bool:
        """Evaluate if a policy allows a specific action on a resource."""
        try:
            policy = self.get(policy_name)
        except ValueError:
            return False

        policy_permissions = {perm.to_dict() for perm in policy.permissions}

        # Check if all required permissions are present in the policy
        for req_perm in permissions:
            if req_perm.to_dict() not in policy_permissions:
                return False
        return True
