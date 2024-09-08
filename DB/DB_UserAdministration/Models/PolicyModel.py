from typing import List

from DB_UserAdministration.Models.permissionModel import Action, Effect, Permission, Resource

# commit
class Policy:
    def __init__(self, name: str, permissions: List[Permission] = None):
        self.name = name
        self.permissions = permissions if permissions != None else []

    def to_dict(self):
        return {
            "policy_id": self.name,
            "permissions": self.permissions,
        }

    def evaluate(self, action: Action, resource: Resource, effect: Effect):
        allowed = False
        # a permission (statement) can either allow or deny an action
        # the evaluate function returns wether non of the policy permissions deny access
        # and at least one permits access
        for permission in self.permissions:
            if (permission.resource.value == resource.value or permission.resource == '*') and permission.action.value == action.value:
                if permission.effect == Effect.DENY:
                    return False
                elif permission.effect == Effect.ALLOW:
                    allowed = True

        return allowed

    def __repr__(self):
        return f'Policy(policy_id=[{self.policy_id}], permissions=[{[Permission.get_permission_by_id(perm) for perm in self.permissions]}])'
