from DB.Service.Classes import PolicyService


class PolicyController:
    def __init__(self, service: PolicyService):
        self.service = service
        
    def create_policy(self, name):
        self.service.create(name)
        
    