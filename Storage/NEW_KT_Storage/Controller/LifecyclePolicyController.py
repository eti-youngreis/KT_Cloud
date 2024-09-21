from Storage.NEW_KT_Storage.Service.Classes.LifecyclePolicyService import LifecyclePolicyService
from Storage.NEW_KT_Storage.Validation.LifecyclePolicyValidations import *
class LifecyclePolicyController:
    def __init__(self):
        self.service = LifecyclePolicyService()

    def create(self, policy_name: str, expiration_days: int =None, transitions_days_GLACIER:int =None, status:str ='Enabled',prefix=[]):
        validate_lifecycle_attributes(policy_name=policy_name, expiration_days=expiration_days, transitions_days_GLACIER=transitions_days_GLACIER, status=status)
        self.service.create(policy_name=policy_name, expiration_days=expiration_days, transitions_days_GLACIER=transitions_days_GLACIER, status=status,prefix=prefix)

    def get(self, policy_name):
        validation_policy_name(policy_name=policy_name)
        return self.service.get(policy_name)

    def delete(self, policy_name):
        validation_policy_name(policy_name=policy_name)
        self.service.delete(policy_name=policy_name)

    def modify(self, policy_name:str, expiration_days: int = None, transitions_days_GLACIER: int = None, status:str = None, prefix = []):
        self.service.modify(policy_name=policy_name, expiration_days= expiration_days, transitions_days_GLACIER=transitions_days_GLACIER,status=status,prefix=prefix)

    def describe(self, policy_name):
        validation_policy_name(policy_name)
        return self.service.describe(policy_name)

