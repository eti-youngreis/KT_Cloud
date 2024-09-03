import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from sqlite3 import OperationalError
from typing import Dict
from DB_UserAdministration.DataAccess import PolicyManager
from DB_UserAdministration.Models.PolicyModel import Policy
from DB_UserAdministration.Validation.PolicyValidation import validate_policy_name

# commit
class PolicyService:
    def __init__(self, policy_manager: PolicyManager):
        self.policy_manager = policy_manager
        self.policies: Dict[str, Policy] = {policy.policy_id: policy for policy in self.policy_manager.get_all()}

    def create(self, policy: Policy):
        try:
            self.policy_manager.insert(policy)
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        self.policies[policy.policy_id] = policy

    def delete(self, name):
        try:
            self.policy_manager.delete_by_id(name)
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

    def subscribe(self, policy: Policy):
        self.policy_manager.subscribe(policy)
        
    def get(self, name):
        try:
            return self.policy_manager.get_by_id(name)
        except OperationalError as ex:
            return 'policy couldn\'t be retrieved'

    def list_policies(self):
        try:
            return list(self.policies.values())
        except OperationalError as ex:
            return 'policy list couldn\'t be retrieved'

    def add_permission(self, policy_name, permission):
        print(policy_name)
        print(permission)
        #validate_policy_name(policy_name)
        policy = self.policy_manager.get_by_id(policy_name)
        policy.add_permission(permission)
        self.update(policy_name, policy)

    # this function might be useless
    def evaluate(self, name, action, resource):  # name is the policy name
        allowed = False
        try:
            allowed = self.policies[name].evaluate(action, resource)
        except KeyError:
            self.policies = self.policy_manager.list_policies()
            try:
                allowed = self.policies[name].evaluate(action, resource)
            except KeyError:
                pass

        return allowed
