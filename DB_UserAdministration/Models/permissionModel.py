from enum import Enum

class Resource(Enum):
    RDS = "Rds"
    STORAGE = "Storage"

class Action(Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    # הוספת פעולות נוספות לפי הצורך, למשל: MODIFY, LIST, CREATE

class permission:

    def __init__(self, action:Action,resource:Resource ) -> None:
        self.action = action
        self.resource = resource

    def to_dict(self):
            return {
                "permission_id": self.permission_id,
                "action": self.action.value,
                "resource": self.resource.value
            }
            
    def __repr__(self):
        return f"Permission(id={self.permission_id}, action='{self.action.value}', resource='{self.resource.value}')"

    # def update_permission(self, action: Action = None, resource: Resource = None):

