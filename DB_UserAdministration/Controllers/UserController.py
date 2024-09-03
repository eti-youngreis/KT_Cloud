from Services import UserService
class UserController:
    def __init__(self, service: UserService):
        self.service = service

    def create_user(self, user_name, password, roles = [], policies = [], quotas = None):
        self.service.create(user_name, password, roles, policies, quotas)

    def delete_user(self, user_id):
        self.service.delete(user_id)

    def update_user_name(self, user_id, user_name):
        self.service.update(user_id, user_name)