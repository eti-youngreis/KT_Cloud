from typing import Optional, List
from Models.UserModel import User
from Models.PermissionModel import Permission
from Models.PolicyModel import Policy
from Models.GroupModel import Group
from Models.QuotaModel import Quota
from Models.RoleModel import Role
from DataAccess.UserDAL import UserDAL
from Storage.DataAccess.QoutaManager import QuotaManager
from Storage_UserAdministration.Services.QuotaService import QuotaService



class UserService:
    def __init__(self, user_dal:UserDAL):
        self.user_dal = user_dal
        Q_dal = QuotaManager()
        self.quota_service =QuotaService(Q_dal)

    def create_user(
        self,
        username: str,
        password: str,
        email: str,
        role: Optional[Role] = None,
        policies: Optional[List[Policy]] = None,
        quotas: Optional[Quota] = None,
        groups: Optional[List[Group]] = None,
    ):
        """Creates a new user with optional role, policies, quotas, and groups."""
        new_user = User(username, password, email, role, policies, quotas, groups)
        return self.user_dal.save_user(new_user)

    def delete_user(self, user_id: str):
        """Deletes a user by ID."""
        return self.user_dal.delete_user(user_id)

    def update_user(
        self, user_id: str, username: Optional[str] = None, email: Optional[str] = None, quotas: Optional[dict]=None
    ):
        """Updates a user's username and/or email."""
        user = self.user_dal.get_user(user_id)
        if user:
            if username:
                user.username = username
            if email:
                user.email = email
            if quotas:
                user.quotas = quotas
            return self.user_dal.update_user(user)
        return None

    def get_user(self, user_id: str):
        """Fetches a user by ID."""
        return self.user_dal.get_user(user_id)

    def list_users(self):
        """Lists all users."""
        return self.user_dal.list_users()

    def assign_permission(self, user_id: str, permission: str):
        """Assigns a permission to a user's role."""
        user = self.user_dal.get_user(user_id)
        if user and user.role:
            user.role.add_permission(permission)
            return self.user_dal.update_user(user)
        return None

    def revoke_permission(self, user_id: str, permission: str):
        """Revokes a permission from a user's role."""
        user = self.user_dal.get_user(user_id)
        if user and user.role:
            user.role.remove_permission(permission)
            return self.user_dal.update_user(user)
        return None

    def add_to_group(self, user_id: str, group: Group):
        """Adds a user to a group."""
        user = self.user_dal.get_user(user_id)
        if user and group.name not in [g.name for g in user.groups]:
            user.groups.append(group)
            return self.user_dal.update_user(user)
        return None

    def remove_from_group(self, user_id: str, group_name: str):
        """Removes a user from a group by name."""
        user = self.user_dal.get_user(user_id)
        if user:
            user.groups = [g for g in user.groups if g.name != group_name]
            return self.user_dal.update_user(user)
        return None

    def set_quota(self, user_id: str, quota: Quota):
        """Sets a quota for a user."""
        user = self.user_dal.get_user(user_id)
        if user:
            user.quota = quota
            return self.user_dal.update_user(user)
        return None

    def get_quota(self, user_id: str):
        """Fetches a user's quota."""
        user = self.user_dal.get_user(user_id)
        return user.quota if user else None

    def add_policy(self, user_id: str, policy: Policy):
        """Adds a policy to a user."""
        user = self.user_dal.get_user(user_id)
        if user:
            user.policies = user.policies or []
            user.policies.append(policy)
            return self.user_dal.update_user(user)
        return None
    
    def add_quota_usage(self, user_id, q_name, amount_usage):
        user = self.user_dal.get_user(user_id)
        if not user:
            return
        
        user_quotas = user.quotas
        user_quota = user_quotas.get(q_name, None) 
        if user_quota is None:
            raise ValueError(f"user dont have quota named {q_name}")
        
        quota = self.quota_service.get(q_name)
        curr_usage = user_quota.value() 
        updated_usage = curr_usage + amount_usage

        if quota.check_exceeded(updated_usage):
            raise ValueError("Cannot add this amount. Limit is exceeded.")
    
        user_quotas[q_name] = updated_usage 
        self.update_user(user_id, quotas=user_quotas)

        

