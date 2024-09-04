# from Models.PermissionModel import Permission
# from Models.PolicyModel import Policy
# from Models.GroupModel import Group
# from Models.QuotaModel import Quota
# from Models.RoleModel import Role
from typing import Optional
import uuid

class User:
    def __init__(
        self,
        username: str,
        password: str,
        email=None,
        
        # role: Optional[Role] = None,
        # policies: Optional[List[Policy]] = None,
        quotas: Optional[dict] = None,
        # groups: Optional[List[Group]] = None
    ):
        self.user_id = str(uuid.uuid4())  # Unique identifier
        self.username = username
        self.password=password
        self.email = email
        self.logged_in=False
        self.token=None
        # self.role = role
        # self.policies = policies
        self.quotas =quotas
        # self.groups = groups


    # def hash_password(self, password):
    #     # Implement password hashing
    #     return hashlib.sha256(password.encode()).hexdigest()

    def verify_password(self, password:str):
        # Verify password against the hashed password
        return self.password_hash == self.hash_password(password)

    # def can(self, action, resource):
    #     return any(
    #         policy.evaluate(policy_name, permissions) for policy in self.policies
    #     ) or self.role.has_permission(permissions)

    # def update_quota(self, quota: Quota):
    #     self.quota = quota

    # def check_quota(self):
    #     return self.quota.check_exceeded()


