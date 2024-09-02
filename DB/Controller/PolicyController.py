from DB.Service.Classes import PolicyService


class PolicyController:
    def __init__(self, service: PolicyService):
        self.service = service
        
    def create_policy(self, name):
        self.service.create(name)
        
    def delete_policy(self, name):
        self.service.delete(name)
    
    def get_policy(self, name):
        return self.service.get(name)
        
    def list_policies(self):
        return self.service.list_policies()
    
    
    