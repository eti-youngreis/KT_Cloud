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
        return self.dal.updte_

    def get_permission(self, permission_id: int):
        pass

    def has_permission(self, action:Action, resource: Resource):
        pass