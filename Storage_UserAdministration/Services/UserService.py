from typing import Optional, List,Dict
from KT_Cloud.Storage_UserAdministration.Models.userModel import User
from KT_Cloud.Storage_UserAdministration.Models.PermissionModel import Permission
from KT_Cloud.Storage_UserAdministration.Models.PolicyModel import PolicyModel
from KT_Cloud.Storage_UserAdministration.Models.GroupModel import Group
from KT_Cloud.Storage_UserAdministration.Controllers.PolicyController import PolicyController
# from KT_Cloud.Storage_UserAdministration.Models.QuotaModel import Quota
# from KT_Cloud.Storage_UserAdministration.Models.RoleModel import Role
from KT_Cloud.Storage_UserAdministration.DataAccess.UserDAL import UserDAL


class UserService:
    def __init__(self, user_dal):
        self.user_dal = user_dal

    def create_user(
        self,
        username: str,
        password: str,
        email: str,
        role: Optional[str] = None,
        policies: Optional[List[str]] = None,
        quotas: Optional[Dict[str:int]] = None,
        groups: Optional[List[str]] = None,
    ):
        """Creates a new user with optional role, policies, quotas, and groups."""
        new_user = User(username, password, email, role, policies, quotas, groups)
        return self.user_dal.save_user(new_user)

    def delete_user(self, username: str):
        """Deletes a user by ID."""
        return self.user_dal.delete_user(username)

    def update_user(
        self, username: str = None, email: Optional[str] = None
    ):
        """Updates a user's username and/or email."""
        user = self.user_dal.get_user(username)
        if user:
            if email:
                user.email = email
            return self.user_dal.update_user(user)
        return None

    def get_user(self, username: str):
        """Fetches a user by ID."""
        return self.user_dal.get_user(username)

    def list_users(self):
        """Lists all users."""
        return self.user_dal.list_users()

    def assign_permission(self, username: str, permission: str):
        """Assigns a permission to a user's role."""
        user = self.user_dal.get_user(username)
        if user and user.role:
            user.role.add_permission(permission)
            return self.user_dal.update_user(user)
        return None

    def revoke_permission(self, username: str, permission: str):
        """Revokes a permission from a user's role."""
        user = self.user_dal.get_user(username)
        if user and user.role:
            user.role.remove_permission(permission)
            return self.user_dal.update_user(user)
        return None

    def add_to_group(self, username: str, group: Group):
        """Adds a user to a group."""
        user = self.user_dal.get_user(username)
        if user and group.name not in [g.name for g in user.groups]:
            user.groups.append(group)
            return self.user_dal.update_user(user)
        return None

    def remove_from_group(self, username: str, group_name: str):
        """Removes a user from a group by name."""
        user = self.user_dal.get_user(username)
        if user:
            user.groups = [g for g in user.groups if g.name != group_name]
            return self.user_dal.update_user(user)
        return None

    def set_quota(self, username: str, quota: enam(str:int)):
        """Sets a quota for a user."""
        user = self.user_dal.get_user(username)
        if user:
            user.quota = quota
            return self.user_dal.update_user(user)
        return None

    def get_quota(self, username: str):
        """Fetches a user's quota."""
        user = self.user_dal.get_user(username)
        return user.quota if user else None

    def add_policy(self, username: str, policy_name: str):
        """Adds a policy to a user."""
        policyController = PolicyController()
        user = self.user_dal.get_user(username)
        if user:
            user.policies = user.policies or []
            user.policies.append(policy_name)
            policyController.add_user()
            return self.user_dal.update_user(user)

        return None