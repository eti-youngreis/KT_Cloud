from typing import List

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

    def evaluate(self, action, resource):
        allowed = False
        denied = False
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
