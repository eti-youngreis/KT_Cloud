from typing import Dict, List
class Acl:
    def __init__(self, owner,permissions):
        self.owner = owner
        self.permissions =permissions or []
    def to_dict(self) -> Dict:
        return {
            "Owner": self.owner,
            "Permissions": self.permissions
        }







