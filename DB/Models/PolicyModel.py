from typing import List


class Policy:
    def __init__(self, name: str, permissions: List[Permission] = None):
        self.name = name
        self.permissions = permissions if permissions != None else []
        
    def to_dict(self):
        return {
            "policy_id": self.name,
            "permissions": [perm.to_dict() for perm in self.permissions]
        }