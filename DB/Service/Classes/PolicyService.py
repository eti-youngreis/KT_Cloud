from sqlite3 import OperationalError
from DB.DataAccess.PolicyManager import PolicyManager
from DB.Models.PolicyModel import Policy
from DB.Validation.PolicyValidation import validate_policy_name


class PolicyService:
    def __init__(self, dal: PolicyManager):
        self.policy_manager = dal

    def create(self, name):
        new_policy = Policy(name)
        try:
            self.policy_manager.create(new_policy.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

    def add_permission(self, policy_name, permission):
        validate_policy_name(policy_name)
        policy = self.policy_manager.get(policy_name)
        policy['permissions'].append(permission)
        self.policy_manager.update(policy_name, policy)
