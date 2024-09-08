from enum import Enum
from bidict import bidict
from itertools import product

class Action(Enum):
    """Enumeration of possible actions for permissions."""
    READ = 'read'
    WRITE = 'write'
    DELETE = 'delete'
    UPDATE = 'update'
    EXECUTE = 'execute'

class Resource(Enum):
    """Enumeration of resources that permissions can apply to."""
    BUCKET = 'bucket'
    DATABASE = 'database'

class Effect(Enum):
    """Enumeration of effects that a permission can have."""
    DENY = "deny"
    ALLOW = "allow"

class Permission:
    """
    Manages permissions with a bidirectional map for quick lookup by ID or permission details.
    Methods:
        get_permission_by_id(permission_id: int) -> dict[str, str]:
            Returns the permission details for the given ID.
        get_id_by_permission(action: Action, resource: Resource, effect: Effect) -> int:
            Returns the ID for the given permission details.
    """
    # Create permissions dynamically with combinations of enums
    _permissions = bidict({
        idx: (action, resource, effect)
        for idx, (action, resource, effect) in enumerate(
            product(Action, Resource, Effect), start=1
        )
    })

    @classmethod
    def get_permission_by_id(cls, permission_id: int) -> dict[str, str]:
        """
        Get the details of the permission associated with the given ID.
        :param permission_id: The ID of the permission.
        :return: A dictionary with the permission details (action, resource, effect).
        :raises PermissionNotFoundError: If the ID is not found in the permissions map.
        """

        action, resource, effect = cls._permissions[permission_id]
        return {
            "action": action.value,
            "resource": resource.value,
            "effect": effect.value
        }

    @classmethod
    def get_id_by_permission(cls, action: Action, resource: Resource, effect: Effect) -> int:
        """
        Get the ID associated with the given permission details using strings.
        :param action_str: The action of the permission as a string (e.g., 'read').
        :param resource_str: The resource of the permission as a string (e.g., 'bucket').
        :param effect_str: The effect of the permission as a string (e.g., 'allow').
        :return: The ID of the permission.
        :raises ValueError: If the permission details are not found in the map.
        """
        try:
            action = Action(str(action.value))
            resource = Resource(str(resource.value))
            effect = Effect(str(effect.value))
        except ValueError as e:
            raise ValueError(f"Invalid permission details: {e}")
        
        permission = (action, resource, effect)
        
        if permission not in cls._permissions.inv:
            raise ValueError("Permission not found.")
        
        return cls._permissions.inv[permission]