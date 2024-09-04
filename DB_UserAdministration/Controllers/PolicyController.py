from typing import Any, Dict
from DB_UserAdministration.Models.permissionModel import Permission
from DB_UserAdministration.Services.PolicyService import PolicyService

# commit
class PolicyController:
    def __init__(self, service: PolicyService):
        self.service = service

    def create_policy(self, name):
        self.service.create(name)

    def add_permission(self, policy_name: str, permission: Permission) -> None:
        self.service.add_permission(policy_name, permission)

    def delete_policy(self, name):
        self.service.delete(name)

    def get_policy(self, name):
        return self.service.get(name)

    def list_policies(self):
        return self.service.list_policies()
