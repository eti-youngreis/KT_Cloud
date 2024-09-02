from permissionModel import Permission, Action, Resource, Effect  
from DataAccess import permissionManager 

class permissionService:
    """
    Service class to manage permissions. This class interacts with the data access layer (DAL) to 
    perform CRUD operations on permissions.
    """

    def __init__(self, dal: permissionManager) -> None:
        """
        Initialize the permissionService with a data access layer instance.
        
        :param dal: An instance of permissionManager that handles data access for permissions.
        """
        self.dal = dal

    def create_permission(self, action: Action, resource: Resource, effect: Effect):
        """
        Create a new permission if it doesn't already exist.
        
        :param action: The action associated with the permission.
        :param resource: The resource associated with the permission.
        :param effect: The effect (e.g., allow or deny) of the permission.
        :raises ValueError: If the permission already exists.
        :return: The created permission data.
        """
        if self.is_exit_permission(action, resource, effect):
            raise ValueError(f'Permission \'{action}, {resource}, {effect}\' already exists.')
        
        # Create a new permission object and pass it to the DAL for creation
        new_permission = Permission(action, resource, effect)
        return self.dal.create_permission(new_permission.to_dict())
    
    def is_exit_permission(self, action, resource, effect):
        """
        Check if a permission already exists based on the action, resource, and effect.
        
        :param action: The action associated with the permission.
        :param resource: The resource associated with the permission.
        :param effect: The effect of the permission.
        :return: True if the permission exists, otherwise False.
        """
        return self.dal.is_exit_permission(action, resource, effect)
    
    def delete_permission(self, permission_id: int):
        """
        Delete a permission by its ID.
        
        :param permission_id: The ID of the permission to delete.
        :return: The result of the deletion operation from the DAL.
        """
        return self.dal.delete_permission(permission_id)
    

    def list_permissions(self):
        """
        List all permissions.
        
        :return: A list of all permissions managed by the DAL.
        """
        return self.dal.list_permissions()

    def update_permission(self, permission_id: int, action: Action, resource: Resource, effect: Effect):
        """
        Update an existing permission's action and resource based on its ID.
        
        :param permission_id: The ID of the permission to update.
        :param action: The new action for the permission.
        :param resource: The new resource for the permission.
        :return: The result of the update operation from the DAL.
        """
        return self.dal.update_permission(permission_id, action, resource)

    def get_permission(self, permission_id: int):
        """
        Retrieve a permission by its ID.
        
        :param permission_id: The ID of the permission to retrieve.
        :return: The permission data from the DAL.
        """
        return self.dal.get_permission(permission_id)
