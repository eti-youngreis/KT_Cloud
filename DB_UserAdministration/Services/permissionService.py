from permissionModel import Permission, Action, Resource
from DataAccess import permissionManager

class permissionService:

    def __init__(self, dal:permissionManager) -> None:
        self.dal = dal

    def create_permission(self, permission_id: int, action:Action, resource: Resource) -> Permission:
        return self.dal.create_permission(permission_id, action, resource)

    def delete_permission(self, permission_id: int):
        return self.dal.delete_permission(permission_id)

    def list_permissions(self):
        return self.list_permissions()

    def update_permission(self, permission_id: int, action:Action, resource: Resource ):
        return self.dal.update_permission(permission_id, action, resource)

    def get_permission(self, permission_id: int):
        return self.dal.get_permission(permission_id)

    def has_permission(self, action:Action, resource: Resource):
        return self.dal.has_permission(action, resource)