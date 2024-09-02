from DB.Service.Classes import PolicyService


class PolicyController:
    def __init__(self, service: PolicyService):
        self.service = service
        
    def create_policy(self, name):
        self.service.create(name)
        
    def add_permission(self, policy_name, permission):
        self.service.add_permission(policy_name, permission)