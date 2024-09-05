from Models.UserModel import User
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
        return selfs.user_service.create_user(username, password, email)

    def delete_user(self, username: str):
        """Delete a user by their user ID."""
        return self.user_service.delete_user(username)

    def update_user(
        self, username: str = None, email: Optional[str] = None
    ):
        """Update user's username and/or email."""
        return self.user_service.update_user(username, email)

    def get_user(self, username: str):
        """Get a user's details by their user ID."""
        return self.user_service.get_user(username)

    def list_users(self):
        """List all users."""
        return self.user_service.list_users()

    def assign_permission(self, username: str, permission: Permission):
        """Assign a permission to a user."""
        return self.user_service.assign_permission(username, permission)

    def revoke_permission(self, username: str, permission: Permission):
        """Revoke a permission from a user."""
        return self.user_service.revoke_permission(username, permission)

    def add_to_group(self, username: str, group: Group):
        """Add a user to a group."""
        return self.user_service.add_to_group(username, group)

    def remove_from_group(self, username: str, group_name: str):
        """Remove a user from a group."""
        return self.user_service.remove_from_group(username, group_name)

    def set_quota(self, username: str, quota: Quota):
        """Set a quota for a user."""
        return self.user_service.set_quota(username, quota)

    def get_quota(self, username: str):
        """Get a user's quota."""
        return self.user_service.get_quota(username)

    def add_policy(self, username: str, policy: Policy):
        """Add a policy to a user."""
        return self.user_service.add_policy(username, policy)

