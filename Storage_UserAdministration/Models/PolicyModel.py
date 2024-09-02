from typing import Dict, List

from Models.PermissionModel import Permission


# from Storage.Models.PermissionModel import Permission

class PolicyModel:
    def __init__(self,policy_name:str, version: str, permissions: List[Permission]=None):
        self.policy_name = policy_name
        self.version = version
        self.permissions = permissions or []

    def to_dict(self) -> Dict:
        return {
            "version": self.version,
            "policyName": self.policy_name,
            "permissions": [permission.to_dict() for permission in self.permissions]
        }
