from enum import Enum
import json
from typing import List

class Action(Enum):
    """
    Enumeration of possible actions for permissions.
    
    Attributes:
        READ: Represents a read action.
        WRITE: Represents a write action.
        DELETE: Represents a delete action.
        UPDATE: Represents an update action.
        EXECUTE: Represents an execute action.
    """
    READ = 'read'
    WRITE = 'write'
    DELETE = 'delete'
    UPDATE = 'update'
    EXECUTE = 'execute'

class Resource(Enum):
    """
    Enumeration of resources that permissions can apply to.
    
    Attributes:
        BUCKET: Represents a bucket resource.
        DATABASE: Represents a database resource.
    """
    BUCKET = 'bucket'
    DATABASE = 'database'

class Effect(Enum):
    """
    Enumeration of effects that a permission can have.
    
    Attributes:
        DENY: Represents a deny effect, restricting access.
        ALLOW: Represents an allow effect, granting access.
    """
    DENY = "deny"
    ALLOW = "allow"

class Permission:
    """
    Represents a permission with an action, resource, and effect.
    
    Attributes:
        action: The action associated with the permission (e.g., read, write).
        resource: The resource that the permission applies to (e.g., bucket, database).
        effect: The effect of the permission (e.g., allow, deny).
    """
    def __init__(self, action: Action, resource: Resource, effect: Effect):
        """
        Initialize a Permission object.
        
        :param action: The action for the permission.
        :param resource: The resource the permission applies to.
        :param effect: The effect of the permission.
        """
        self.action = action
        self.resource = resource
        self.effect = effect
       
    def to_dict(self):
        """
        Convert the permission object to a dictionary format.
        
        :return: A dictionary with the permission's action, resource, and effect.
        """
        return {
            "action": self.action.value,  # Get the string value of the action enum
            "resource": self.resource.value,  # Get the string value of the resource enum
            "effect": self.effect.value  # Get the string value of the effect enum
        }

    def __repr__(self):
        """
        Return a string representation of the permission object.
        
        :return: A formatted string displaying the permission's details.
        """
        return f"Permission(action='{self.action.value}', resource='{self.resource.value}', effect='{self.effect.value}')"

    def update_permission(self, action: Action = None, resource: Resource = None, effect: Effect = None):
        """
        Update the permission's action, resource, or effect.
        
        :param action: The new action for the permission (optional).
        :param resource: The new resource for the permission (optional).
        :param effect: The new effect for the permission (optional).
        """
        if action:
            self.action = action  # Update the action if a new one is provided
        if resource:
            self.resource = resource  # Update the resource if a new one is provided
        if effect:
            self.effect = effect  # Update the effect if a new one is provided

    @classmethod
    def build_from_dict(cls, dict):
        try:
            return cls(
                action=Action(dict['action'].upper()),
                resource=Resource(dict['resource'].upper()),
                effect=Effect(dict['effect'].upper())
            )
        except Exception as ex:
            raise ex
        
        
   