from sqlite3 import OperationalError
from DB.DataAccess import PolicyManager
from DB.Models.PolicyModel import Policy


class PolicyService:
    def __init__(self, policy_manager: PolicyManager):
        self.policy_manager = policy_manager
        
    def create(self, name):
        new_policy = Policy(name)
        try:
            self.policy_manager.create(new_policy.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}') 
        
    def delete(self, name):
        try:
            self.policy_manager.delete(name)
        except OperationalError as ex:
            return 'policy couldn\'t be deleted'
        
    
    def update(self, name, permissions):
        try:
            self.policy_manager.update(name, Policy(name, permissions=permissions))
        except OperationalError as ex:
            return 'policy couldn\'t be updated'
        
    def get(self, name):
        try:
            return self.policy_manager.get(name)
        except OperationalError as ex:
            return 'policy couldn\'t be retrieved'
        
    def list_policies(self):
        try:
            return self.policy_manager.get_all_policies()
        except OperationalError as ex:
            return 'policy list couldn\'t be retrieved'