from Models.userModel import User
from Models.PermissionModel import Permission
from Models.PolicyModel import Policy
from Models.GroupModel import Group
from Models.QuotaModel import Quota
from Models.RoleModel import Role
from Services.UserService import UserService

class UserController:
    def __init__(self, user_service):
        self.user_service = user_service

    def create_user(self, username: str, password: str, email: str):
        """Create a new user."""
        return self.user_service.create_user(username, password, email)

    def delete_user(self, user_id: str):
        """Delete a user by their user ID."""
        return self.user_service.delete_user(user_id)

    def update_user(
        self, user_id: str, username: Optional[str] = None, email: Optional[str] = None
    ):
        """Update user's username and/or email."""
        return self.user_service.update_user(user_id, username, email)

    def get_user(self, user_id: str):
        """Get a user's details by their user ID."""
        return self.user_service.get_user(user_id)

    def list_users(self):
        """List all users."""
        return self.user_service.list_users()

    def assign_permission(self, user_id: str, permission: Permission):
        """Assign a permission to a user."""
        return self.user_service.assign_permission(user_id, permission)

    def revoke_permission(self, user_id: str, permission: Permission):
        """Revoke a permission from a user."""
        return self.user_service.revoke_permission(user_id, permission)

    def add_to_group(self, user_id: str, group: Group):
        """Add a user to a group."""
        return self.user_service.add_to_group(user_id, group)

    def remove_from_group(self, user_id: str, group_name: str):
        """Remove a user from a group."""
        return self.user_service.remove_from_group(user_id, group_name)

    def set_quota(self, user_id: str, quota: Quota):
        """Set a quota for a user."""
        return self.user_service.set_quota(user_id, quota)

    def get_quota(self, user_id: str):
        """Get a user's quota."""
        return self.user_service.get_quota(user_id)

    def add_policy(self, user_id: str, policy: Policy):
        """Add a policy to a user."""
        return self.user_service.add_policy(user_id, policy)

