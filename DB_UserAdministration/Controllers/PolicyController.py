import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DB_UserAdministration.Services import PolicyService
from DB_UserAdministration.Models.PolicyModel import Policy

# commit
class PolicyController:
    def __init__(self, service: PolicyService):
        self.service = service

    def create_policy(self, policy: Policy):
        self.service.create(policy)

    def add_permission(self, policy_name, permission):
        self.service.add_permission(policy_name, permission)

    def delete_policy(self, name):
        self.service.delete(name)

    def get_policy(self, name):
        return self.service.get(name)

    def list_policies(self):
        return self.service.list_policies()
    
    def subscribe(self, policy: Policy):
        self.service.subscribe(policy)
