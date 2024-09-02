from Services import UserService
class UserController:
    def __init__(self, service: UserService):
        self.service = service

    def create_user(self, userName, password, roles = [], policies = [], quotas = None):
        self.service.create(userName, password, roles, policies, quotas)

    def delete_user(self, cluster_identifier: str):
        self.service.delete(cluster_identifier)