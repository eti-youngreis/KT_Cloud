import json
from typing import Dict, List
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from DB_UserAdministration.Models.permissionModel import Action, Effect, Permission, Resource

# commit
class Policy:
    def __init__(self, policy_id: str, permissions: List[int] = None):
        self.policy_id = policy_id
        self.permissions = permissions
        
        
    def to_dict(self):
        return {
            "policy_id": self.policy_id,
            "permissions": self.permissions,
        }

    def __repr__(self):
        return f"Policy(policy_id={self.policy_id!r}, permissions={self.permissions})"
    
    def evaluate(self, action, resource):
        allowed = False
        # a permission (statement) can either allow or deny an action
        # the evaluate function returns wether non of the policy permissions deny access
        # and at least one permits access
        for permission in self.permissions:
            perm = Permission._permissions[permission]
            if (perm.resource == resource or perm.resource == '*') and perm.action == action:
                if perm.effect == Effect.DENY:
                    return False
                elif perm.effect == Effect.ALLOW:
                    allowed = True

        return allowed
    
    @staticmethod
    def build_from_dict(dict):
        id = dict['policy_id']
        permissions = [Permission.get_permission_by_id(perm) for perm in json.loads(dict['permissions'])]
        return Policy(id, permissions)

    def add_permission(self, permission: tuple):
        self.permissions.append(Permission.get_id_by_permission(permission[0], permission[1], permission[2]))