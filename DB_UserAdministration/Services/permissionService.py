from permissionModel import Permission, Action, Resource, Effect
from DataAccess import permissionManager

class permissionService:

    def __init__(self, dal:permissionManager) -> None:
        self.dal = dal

    def create_permission(self, action:Action, resource: Resource, effect:Effect):
        if self.is_exit_permission(action, resource, effect):
             raise ValueError(f'permission \'{action}, {resource}, {effect}\' already exists.')
        new_permission = Permission(action, resource, effect)
        return self.dal.create_permission(new_permission.to_dict())
    
    def is_exit_permission(self, action, resource, effect):
        return self.dal.is_exit_permission(action, resource, effect)
    
    def delete_permission(self, permission_id: int):
        return self.dal.delete_permission(permission_id)

    def list_permissions(self):
        return self.dal.list_permissions()

    def update_permission(self, permission_id: int, action:Action, resource: Resource ):
        return self.dal.update_permission(permission_id, action, resource)

    # def get_permission(self, permission_id: int):
    #     return self.dal.get_permission(permission_id)
