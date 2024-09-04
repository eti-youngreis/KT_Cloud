from typing import Dict, List

from Storage_UserAdministration.Models.PermissionModel import Permission

class PolicyModel:
    def __init__(self,policy_name:str, version: str, permissions: List[Permission]=None):
        self.policy_name = policy_name
        self.version = version
        self.permissions = permissions or []

    def to_dict(self) -> Dict:
        return {
            "version": self.version,
            "policyName": self.policy_name,
            "permissions": [
                permission.to_dict() if isinstance(permission, Permission) else permission
                for permission in self.permissions
            ]
        }


