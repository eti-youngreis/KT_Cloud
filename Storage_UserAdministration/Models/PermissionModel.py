from enum import Enum
import uuid

class StorageAction(Enum):
    READ = 'read'
    WRITE = 'write'
    DELETE = 'delete'
    UPDATE = 'update'
    LIST = 'list'

class StorageResource(Enum):
    BUCKET = 'bucket'
    OBJECT = 'object'

class Effect(Enum):
    DENY = "deny"
    ALLOW = "allow"

class StoragePermission:
    """
    Represents a permission with an ID, name, action, resource, and effect for the storage system.
    
    Attributes:
        id: Unique identifier for the permission.
        name: A human-readable name for the permission.
        action: The action associated with the permission.
        resource: The resource that the permission applies to.
        effect: The effect of the permission.
    """
    def __init__(self, name: str, action: StorageAction, resource: StorageResource, effect: Effect, id: str = None):
        """
        Initialize a StoragePermission object.
        
        :param id: The unique identifier for the permission. If not provided, a new UUID will be generated.
        :param name: The name for the permission.
        :param action: The action for the permission.
        :param resource: The resource the permission applies to.
        :param effect: The effect of the permission.
        """
        self.id = id or str(uuid.uuid4())  # Generate a UUID if no id is provided
        self.name = name
        self.action = action
        self.resource = resource
        self.effect = effect

    def to_dict(self):
        """
        Convert the permission object to a dictionary format.
        
        :return: A dictionary with the permission's ID, name, action, resource, and effect.
        """
        return {
            "id": self.id,
            "name": self.name,
            "action": self.action.value,
            "resource": self.resource.value,
            "effect": self.effect.value
        }

    def __repr__(self):
        """
        Return a string representation of the permission object.
        
        :return: A formatted string displaying the permission's details.
        """
        return f"StoragePermission(id='{self.id}', name='{self.name}', action='{self.action.value}', resource='{self.resource.value}', effect='{self.effect.value}')"

    def update_permission(self, name: str = None, action: StorageAction = None, resource: StorageResource = None, effect: Effect = None):
        """
        Update the permission's name, action, resource, or effect.
        
        :param name: The new name for the permission (optional).
        :param action: The new action for the permission (optional).
        :param resource: The new resource for the permission (optional).
        :param effect: The new effect for the permission (optional).
        """
        if name:
            self.name = name
        if action:
            self.action = action
        if resource:
            self.resource = resource
        if effect:
            self.effect = effect
