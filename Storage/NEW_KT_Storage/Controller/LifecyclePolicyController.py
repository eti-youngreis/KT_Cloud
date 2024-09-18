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

    def modify(self, policy_name:str, expiration_days:int=None, transitions_days_GLACIER:int=None, status:str=None, prefix=[]):
        validate_lifecycle_attributes(policy_name=policy_name,expiration_days=expiration_days,transitions_days_GLACIER=transitions_days_GLACIER,status=status)
        #prefix valid
        self.service.modify(policy_name=policy_name, expiration_days= expiration_days, transitions_days_GLACIER=transitions_days_GLACIER,status=status,prefix=prefix)

    def describe(self, policy_name):
        validation_policy_name(policy_name)
        return self.service.describe(policy_name)

def main():
    controller = LifecyclePolicyController()

    # controller.create(policy_name="Policy7", expiration_days=3, transitions_days_GLACIER=1,status="Enabled")

    policy = controller.get(policy_name="MyPolicy")
    print(f"Policy retrieved: {policy}")

    #description = controller.describe(policy_name="MyPolicy")
    #print(f"Policy description: {description}")

    #controller.modify(policy_name="MyPolicy", expiration_days=3, status="Disabled")

    # controller.delete(policy_name="MyPolicy")

if __name__ == "__main__":
    main()
