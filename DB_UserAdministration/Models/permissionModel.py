from enum import Enum

class Action(Enum):
    READ = 'read'
    WRITE = 'write'
    DELETE = 'delete'
    UPDATE = 'update'
    EXECUTE = 'execute'

class Resource(Enum):
    FILE = 'file'
    BUCKET = 'bucket'
    DATABASE = 'database'
    SERVICE = 'service'


class Permission:
    def __init__(self, action: Action, resource: Resource, effect:str):
        self.action = action
        self.resource = resource
        # if Validation.string_in_dict(effect, {'Allow','Deny'}) :
        #     self.effect = effect
        # else


        Allow 
    Deny
   
    def to_dict(self):
        return {
            "permission_id": self.permission_id,
            "action": self.action.value,
            "resource": self.resource.value
        }

    def __repr__(self):
        return f"Permission(id={self.permission_id}, action='{self.action.value}', resource='{self.resource.value}')"

    def update_permission(self, action: Action = None, resource: Resource = None):
        if action:
            self.action = action
        if resource:
            self.resource = resource
