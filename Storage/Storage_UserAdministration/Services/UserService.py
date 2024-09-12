from typing import Optional, List, Dict
from Storage.Storage_UserAdministration.Models.userModel import User
from Storage.Storage_UserAdministration.Controllers.PolicyController import PolicyController
from Storage.Storage_UserAdministration.Controllers.QuotaController import QuotaController

class UserService:
    def __init__(self, user_dal,policy_controller:PolicyController,quota_controller:QuotaController):
        self.user_dal = user_dal
        self.policy_controller = policy_controller
        self.quota_controller = quota_controller

    def create_user(
        self,
        username: str,
        password: str,
        email: str,
        role: Optional[str] = None,
        policies: Optional[List[str]] = None,
        quotas: Optional[Dict[str, int]] = None,
        groups: Optional[List[str]] = None,
    ) -> User:
        """Creates a new user with optional role, policies, quotas, and groups."""
        new_user = User(username, password, email, role, policies, quotas, groups)
        return self.user_dal.save_user(new_user)

    def delete_user(self, username: str) -> bool:
        """Deletes a user by username."""
        user = self.get_user(username)
        if user:
            if user.policies:
                for policy_name in user.policies:
                    self.policy_controller.delete_policy(policy_name)
            return self.user_dal.delete_user(username)
        return False

    def update_user(self, username: str, email: Optional[str] = None) -> Optional[User]:
        """Updates a user's email."""
        user = self.get_user(username)
        if user and email:
            user.email = email
            return self.user_dal.update_user(user)
        return None

    def get_user(self, username: str) -> Optional[User]:
        """Fetches a user by username."""
        return self.user_dal.get_user(username)

    def list_users(self) -> List[User]:
        """Lists all users."""
        return self.user_dal.list_users()

    def assign_permission(self, username: str, permission: str) -> Optional[User]:
        """Assigns a permission to a user's role."""
        user = self.get_user(username)
        if user and user.role:
            # Add permission to user.role
            # user.role.add_permission(permission)
            return self.user_dal.update_user(user)
        return None

    def revoke_permission(self, username: str, permission: str) -> Optional[User]:
        """Revokes a permission from a user's role."""
        user = self.get_user(username)
        if user and user.role:
            # Remove permission from user.role
            # user.role.remove_permission(permission)
            return self.user_dal.update_user(user)
        return None

    def add_to_group(self, username: str, group: str) -> Optional[User]:
        """Adds a user to a group."""
        user = self.get_user(username)
        if user and group.name not in (g.name for g in user.groups):
            user.groups.append(group)
            return self.user_dal.update_user(user)
        return None

    def remove_from_group(self, username: str, group_name: str) -> Optional[User]:
        """Removes a user from a group."""
        user = self.get_user(username)
        if user:
            user.groups = [g for g in user.groups if g.name != group_name]
            return self.user_dal.update_user(user)
        return None

    def set_quota(self, username: str, quota_name: str) -> Optional[User]:
        """Sets a quota for a user."""
        user = self.get_user(username)
        if user and quota_name not in (user.policies or []):
            user.policies.append(quota_name)
            self.quota_controller.add_entity(quota_name, "user", user.user_id)
            return self.user_dal.update_user(user)
        return None

    def get_quota(self, username: str) -> Optional[Dict[str, int]]:
        """Fetches a user's quota."""
        user = self.get_user(username)
        return user.quotas if user else None

    def add_policy(self, username: str, policy_name: str) -> Optional[User]:
        """Adds a policy to a user."""
        user = self.get_user(username)
        if user and policy_name not in (user.policies or []):
            user.policies.append(policy_name)
            self.policy_controller.add_entity(policy_name, "user", username)
            return self.user_dal.update_user(user)
        return None

    def get_user(self, username: str) -> Optional[User]:
        """Helper method to fetch user by username or return None."""
        return self.user_dal.get_user(username)

