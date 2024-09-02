from sqlite3 import OperationalError
from DB.DataAccess import PolicyManager
from DB.Models.PolicyModel import Policy


class PolicyService:
    def __init__(self, dal: PolicyManager):
        self.policy_manager = dal
        
    def create(self, name):
        new_policy = Policy(name)
        try:
            self.dal.create(new_policy.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}') 