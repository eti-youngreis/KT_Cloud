from permissionModel import Permission, Action, Resource, Effect
class permissionController:

    def __init__(self, service) -> None:
        self.service = service

    def create_permission(self, action:Action, resource: Resource, effect:Effect):
        return self.service.create_permission(action, resource, effect)
    
    def delete_permission(self, permission_id: int):
        return self.service.delete_permission(permission_id)
    
    def update_permission(self):
        return self.service.update_permission()
    
    def list_permissions(self):
        return self.service.list_permissions()

    def get_permission(self, permission_id:int):
        return self.service.get_permission(permission_id)