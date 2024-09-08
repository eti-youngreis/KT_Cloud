from Services import UserService
class UserController:
    def __init__(self, service: UserService):
        self.service = service

    def create_user(self, user_name, password, roles = [], policies = [], quotas = {}):
        self.service.create(user_name, password, roles, policies, quotas)

    def delete_user(self, user_id):
        self.service.delete(user_id)

    def update_user_name(self, user_id, user_name):
        self.service.modify(user_id, user_name)

    def get_user_details(self, user_id):
        self.service.describe(user_id)

    def list_users(self, requesting_user_id):
        self.service.get_all_users(requesting_user_id)


    def assign_policy(self, requesting_user_id, target_user_id, policy):
        self.service.assign_policy(requesting_user_id, target_user_id, policy)

    def revoke_policy(self, requesting_user_id, target_user_id, policy):
        self.service.revoke_policy(requesting_user_id ,target_user_id, policy)

    def add_to_group(self, requesting_user_id, target_user_id, group_name):
        self.service.add_to_group(requesting_user_id, target_user_id, group_name)

    def remove_from_group(self, requesting_user_id, target_user_id, group_id):
        self.service.remove_from_group(requesting_user_id,target_user_id, group_id)

    def add_quota_usage(self, requesting_user_id, target_user_id, resource_type, amount):
        pass

    def get_quotas(self, requesting_user_id, target_user_id):
        self.service.get_quotas(requesting_user_id, target_user_id)

    def add_quota(self, requesting_user_id, target_user_id, quota):
        self.service.add_quota(requesting_user_id, target_user_id, quota)

    def remove_quota(self, requesting_user_id, target_user_id, quota):
        self.service.add_quota(requesting_user_id, target_user_id, quota)

    def check_quota(self, requesting_user_id, target_user_id, quota_resource_type, amount):
        self.service.check_quota(requesting_user_id, target_user_id, quota_resource_type, amount)

    def verify_password(self, requesting_user_id, target_user_id, password):
        pass

