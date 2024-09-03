import json
from typing import List

from Models.permissionModel import Permission

# commit
class Policy:
    def __init__(self, policy_id: str, permissions: List[Permission] = None):
        self.policy_id = policy_id
        self.permissions = permissions if permissions != None else []

    def to_dict(self):
        return {
            "policy_id": self.policy_id,
            "permissions": [perm.to_dict() for perm in self.permissions],
        }

    def __repr__(self):
        permissions_repr = ', '.join(p.__repr__() for p in self.permissions) if len(self.permissions) > 0 else ""
        return f"Policy(policy_id={self.policy_id!r}, permissions=[{permissions_repr}])"
    
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
        id = dict['policy_id']
        permissions = [Permission.build_from_dict(perm) for perm in json.loads(dict['permissions'])]
        return Policy(id, permissions)

    def add_permission(self, permission: Permission):
        self.permissions.append(permission)