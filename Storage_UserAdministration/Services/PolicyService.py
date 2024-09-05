from typing import List
from Storage_UserAdministration.DataAccess.policyManager import PolicyManager
from Storage_UserAdministration.Models.PermissionModel import Permission, Action, Resource, Effect
from Storage_UserAdministration.Models.PolicyModel import PolicyModel
from Storage_UserAdministration.Storage_ML.LRUCache import LRUCache

class PolicyService:
    def __init__(self, dal: PolicyManager, cache_capacity: int = 100):
        self.dal = dal
        self.cache = LRUCache(cache_capacity)

    def create(self, policy_name, version, all_permissions, users=None, groups=None, roles=None):
        """Create a new policy."""
        permissions = [Permission.get_id_by_permission(per[0], per[1], per[2]) for per in all_permissions]
        if not policy_name or not version:
            raise ValueError("Policy name and version are required.")
        if self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' already exists.")
        policy = PolicyModel(policy_name, version, permissions, users=users, groups=groups, roles=roles)
        self.dal.insert(policy.to_dict())
        self.cache.put(policy_name, policy)
        return policy

    def delete(self, policy_name: str):
        """Delete an existing policy."""
        if not self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        self.dal.delete(policy_name)
        self.cache.cache.pop(policy_name, None)

    def update(self, policy_name: str, version: str, all_permissions: list, users= None, groups=None, roles=None) -> PolicyModel:
        """Update an existing policy."""
        permissions = [Permission.get_id_by_permission(per[0], per[1], per[2]) for per in all_permissions]
        if not self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        current_data = self.dal.select(policy_name)
        updated_version = version or current_data.get('version')
        updated_permissions = permissions or [Permission.from_dict(p) for p in current_data.get('permissions', [])]
        updated_users = users or current_data.users
        updated_groups = groups or current_data.groups
        updated_roles = roles or current_data.roles
        updated_policy = PolicyModel(policy_name, updated_version, updated_permissions,updated_users, updated_groups, updated_roles)
        self.cache.put(policy_name, updated_policy)
        return self.dal.update(updated_policy)

    def get(self, policy_name: str) -> PolicyModel:
        """Get a policy by name."""
        policy = self.cache.get(policy_name)
        if policy is None:
            data = self.dal.select(policy_name)
            if data is None:
                raise ValueError(f"Policy '{policy_name}' does not exist.")
            users = data.users or []
            policy = PolicyModel(data.policy_name, data.version, [Permission.get_permission_by_id(per) for per in data.permissions], users)
            self.cache.put(policy_name, policy)
        return policy

    def list_policies(self) -> List[PolicyModel]:
        """List all policies."""
        policies = self.dal.list_all()
        return [PolicyModel(p.policy_name, p.version, [per for per in p.permissions]) for p in policies]

    def add_permission(self, policy_name: str, action, resource, effect):
        """Add a permission to an existing policy."""
        permission = Permission.get_id_by_permission(action, resource, effect)
        if not self.dal.exists(policy_name):
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        current_data = self.dal.select(policy_name)
        # Append the new permission to the existing permissions
        permissions = current_data.permissions
        users = current_data.users or []
        if permission not in permissions:
            permissions.append(permission)
            # Create a new updated policy object with the new list of permissions
            updated_policy = PolicyModel(policy_name, current_data.version, permissions, users)
            # Update the policy in the data access layer
            self.dal.update(updated_policy)
            self.cache.put(policy_name, updated_policy)
        else:
            raise ValueError(f"permission with id: {permission} is already exists")

    def evaluate(self, policy_name: str, action: Action, resource: Resource) -> bool:
        """Evaluate if a policy allows the required permissions."""
        policy = self.get(policy_name)
        if not policy:
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        for permission in policy.permissions:
            if permission["action"] == action.value and permission["resource"] == resource.value and permission["effect"] == Effect.ALLOW.value:
                return True
        return False

    def add_user(self, policy_name: str, user: str):
        """Add a user to an existing policy."""
        policy = self.dal.select(policy_name)
        if not policy:
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        if user not in policy.users:
            policy.users.append(user)
        updated_policy = self.dal.update(policy)
        self.cache.put(policy_name, updated_policy)

    def delete_user(self,policy_name, user):
        """delete user from a policy"""
        policy = self.dal.select(policy_name)
        if not policy:
            raise ValueError(f"Policy '{policy_name}' does not exist.")
        if user not in policy.users:
            raise ValueError(f"user '{user}' does not exist in policy.")
        policy.users.remove(user)
        updated_policy = self.dal.update(policy)
        self.cache.put(policy_name, updated_policy)



