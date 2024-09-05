from typing import List,Optional, Dict
from KT_Cloud.Storage_UserAdministration.Controllers.PolicyController import PolicyController
import uuid

class User:
    def __init__(
        self,
        username: str,
        password: str,
        user_id =None,
        email=None,
        logged_in=False,
        token = None,
        role: Optional[str] = None,
        policies: Optional[List[str]] = None,
        quotas: Optional[Dict[str, int]] = None,
        groups: Optional[List[str]] = None
    ):
        self.user_id = str(uuid.uuid4())  # Unique identifier
        self.username = username
        self.password=password
        self.email = email
        self.logged_in=logged_in
        self.token=token
        self.role = role
        self.policies = policies
        self.quotas =quotas
        self.groups = groups

    # def verify_password(self, password:str):
    #     # Verify password against the hashed password
    #     return self.password_hash == self.hash_password(password)

    def can(self):
        policy_controller = PolicyController()
        policies = []
        for policy_name in self.policies:
            policies.push(policy_controller.get_policy(policy_name))
        for policy in policies:
            if policy_controller.evaluate(policy.policy_name, policy.permissions) == False:
                return False
            # if self.role.has_permission(policy.permissions) ==False:
            #     return False
        return True


