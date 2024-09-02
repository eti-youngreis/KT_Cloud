from permissionModel import Permission, Action, Resource, Effect
class permissionController:

    def __init__(self, service) -> None:
        self.service = service

    def create_permission(self, action:Action, resource: Resource, effect:Effect):
        return self.service.create_permission(action, resource, effect)
    
    def delete_permission(self, permission_id: int):
        return self.service.delete_permission(permission_id)