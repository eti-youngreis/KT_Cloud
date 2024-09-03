import json
from typing import List

from Models.permissionModel import Permission

# commit
class Policy:
    def __init__(self, name: str, permissions: List[Permission] = None):
        self.name = name
        self.permissions = permissions if permissions != None else []

    def to_dict(self):
        return {
            "policy_id": self.name,
            "permissions": [perm.to_dict() for perm in self.permissions],
        }

    def __repr__(self):
        permissions_repr = ', '.join(repr(p) for p in self.permissions)
        return f"Policy(name={self.name!r}, permissions=[{permissions_repr}])"
    
    def evaluate(self, action, resource):
        allowed = False
        # a permission (statement) can either allow or deny an action
        # the evaluate function returns wether non of the policy permissions deny access
        # and at least one permits access
        for permission in self.permissions:
            if (permission.resource == resource or permission.resource == '*') and permission.action == action:
                if permission.effect == "Deny":
                    return False
                elif permission.effect == "Allow":
                    allowed = True

        return allowed
    
    @staticmethod
    def build_from_dict(dict):
        return Policy(dict['policy_id'], list(Permission.build_from_dict(per) for per in dict['permissions']))
