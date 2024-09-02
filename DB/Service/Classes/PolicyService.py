from sqlite3 import OperationalError
from typing import Dict, List
from DB.DataAccess import PolicyManager
from DB.Models.PolicyModel import Policy


class PolicyService:
    def __init__(self, policy_manager: PolicyManager):
        self.policy_manager = policy_manager
        self.policies: Dict[str, Policy] = {}
        
    def create(self, name):
        new_policy = Policy(name)
        try:
            self.policy_manager.create(new_policy.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}') 
        self.policies[name] = new_policy
        
    def delete(self, name):
        try:
            self.policy_manager.delete(name)
        except OperationalError as ex:
            return 'policy couldn\'t be deleted'
        
        del self.policies[name]
        
    
    def update(self, name, permissions): 
        updated_policy = Policy(name, permissions=permissions)
        try:
            self.policy_manager.update(name, updated_policy)
        except OperationalError as ex:
            return 'policy couldn\'t be updated'
        self.policies[name] = updated_policy
        
    def get(self, name):
        try:
            return self.policy_manager.get(name)
        except OperationalError as ex:
            return 'policy couldn\'t be retrieved'
        
    def list_policies(self):
        try:
            return self.policies
        except OperationalError as ex:
            return 'policy list couldn\'t be retrieved'
        
    def evaluate(self, action, resource, conditions):
        allowed = False
        denied = False
        for policy in self.policies:
            for permission in policy.permissions:
                if permission.evaluate(action, resource, conditions):
                    allowed = True
                else:
                    denied = True
        return allowed or not denied