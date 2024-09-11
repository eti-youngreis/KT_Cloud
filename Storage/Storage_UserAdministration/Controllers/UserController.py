from typing import Optional
from Storage.Storage_UserAdministration.Models.PermissionModel import Permission
from Storage.Storage_UserAdministration.Models.GroupModel import Group
from Storage.Storage_UserAdministration.Models.userModel import User
from Storage.Storage_UserAdministration.DataAccess.UserDAL import UserDAL
from Storage.Storage_UserAdministration.DataAccess.QuotaManager import QuotaManager
from Storage.Storage_UserAdministration.DataAccess.policyManager import PolicyManager
from Storage.Storage_UserAdministration.Services.UserService import UserService
from Storage.Storage_UserAdministration.Services.PolicyService import PolicyService
from Storage.Storage_UserAdministration.Services.QuotaService import QuotaService
from Storage.Storage_UserAdministration.Controllers.QuotaController import QuotaController
from Storage.Storage_UserAdministration.Controllers.PolicyController import PolicyController



class UserController:
    def __init__(self, user_service):
        self.user_service = user_service

    def create_user(self, username: str, password: str, email: str):
        """Create a new user."""
        return self.user_service.create_user(username, password, email)

    def delete_user(self, username: str):
        """Delete a user by their user ID."""
        return self.user_service.delete_user(username)

    def update_user(
        self, username: str, email: Optional[str] = None
    ):
        """Update user's username and/or email."""
        return self.user_service.update_user( username, email)

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

    def set_quota(self, username: str, quota: str):
        """Set a quota for a user."""
        return self.user_service.set_quota(username, quota)

    def get_quota(self, username: str):
        """Get a user's quota."""
        return self.user_service.get_quota(username)

    def add_policy(self, username: str, policy: str):
        """Add a policy to a user."""
        return self.user_service.add_policy(username, policy)

def main():

    user_dal = UserDAL("C://Users//User//Downloads//users.json")
    policy_dal = PolicyManager("C://Users//User//Downloads//users.json")
    quota_dal=QuotaManager("C://Users//User//Downloads//users.json")
    policy_service=PolicyService(policy_dal)
    quota_service=QuotaService(quota_dal)
    policy_controller =PolicyController(policy_service)
    quota_controller =QuotaController(quota_service)
    user_service=UserService(user_dal,policy_controller,quota_controller)
    user_controller = UserController(user_service)
    # Print all users
    # print("All Users:")
    users = user_controller.list_users()
    for username, user in users.items():
       print(f"{username}: {user}")

    # Add a new user
    user_controller.create_user(username="new_user", password="password123",  email="new_user@example.com")
    new_user = user_controller.get_user("new_user")
    print(f"Added new user: {new_user.username}")

    # # Update user information
    new_user.email = "updated_user@example.com"
    user_controller.update_user(new_user)
    print(f"Updated user's email to: {new_user.email}")
    #
    # # Add permission to the user

    #

    # # Delete a user
    deleted_user = user_controller.delete_user("new_user")
    if deleted_user:
    #     deleted_user=user_controller.get_user("new_user")
        print(f"Deleted user: {deleted_user.username}")
main()