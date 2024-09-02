from enum import Enum

class Action(Enum):
    READ = 'read'
    WRITE = 'write'
    DELETE = 'delete'
    UPDATE = 'update'
    EXECUTE = 'execute'

class Resource(Enum):
    BUCKET = 'bucket'
    DATABASE = 'database'

class Effect(Enum):
    DENY = "deny"
    ALLOW = "allow"


class Permission:
    def __init__(self, action: Action, resource: Resource, effect:Effect):
        self.action = action
        self.resource = resource
        self.effect = effect
       

    def to_dict(self):
        return {
            "action": self.action.value,
            "resource": self.resource.value,
            "effect":self.effect.value
        }

    def __repr__(self):
        return f"Permission(id={self.permission_id}, action='{self.action.value}', resource='{self.resource.value}')"

    def update_permission(self, action: Action = None, resource: Resource = None, effect:Effect = None):
        if action:
            self.action = action
        if resource:
            self.resource = resource
        if effect:
            self.effect = effect
