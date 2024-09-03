from typing import List

from Storage_UserAdministration.DataAccess.policyManager import PolicyManager
from Storage_UserAdministration.Models.PolicyModel import PolicyModel
from Storage_UserAdministration.Models.PermissionModel import Permission
from Storage_UserAdministration.Services.PolicyService import PolicyService


class PolicyController:
    def __init__(self, service: PolicyService):
        self.service = service

    def create_policy(self, policy_name: str, version: str, permissions: List[Permission] = None) -> PolicyModel:
        """Create a new policy."""
        return self.service.create(policy_name, version, permissions)

    
    def delete_policy(self, policy_name: str) -> str:
        """Delete an existing policy."""
        return self.service.delete(policy_name)

    def update_policy(self, policy_name: str, version: str = None, permissions: List[Permission] = None) -> (str, PolicyModel):
        """Update an existing policy."""
        return self.service.update(policy_name, version, permissions)

    # working on it
    def get_policy(self, policy_name: str) -> PolicyModel:
        """Get a policy by name."""
        return self.service.get(policy_name)

    def list_policies(self) -> List[PolicyModel]:
        """List all policies."""
        return self.service.list_policies()

    def add_permission(self, policy_name: str, permission: Permission):
        """Add a permission to an existing policy."""
        self.service.add_permission(policy_name, permission)

    def evaluate_policy(self, policy_name: str, permissions: List[Permission]) -> bool:
        """Evaluate if a policy allows the required permissions."""
        return self.service.evaluate(policy_name, permissions)

def maim():

    storage = PolicyManager()

    service = PolicyService(storage)

    controller = PolicyController(service=service)

    # create a new permission
    permission1 = Permission(action="s3:GetObject", resource="arn:aws:s3:::bucket1/*", effect="Allow")
    permission2 = Permission(action="s3:PutObject", resource="arn:aws:s3:::bucket1/*", effect="Deny")

    # create a new policy
    new_policy = controller.create_policy(policy_name="ExamplePolicy", version="2024-09-01", permissions=[permission1, permission2])
    print("Created Policy:", new_policy.to_dict())

    # update a policy
    updated_policy = controller.update_policy(policy_name="ExamplePolicy", version="2024-09-03", permissions=[permission1])
    print("Updated Policy:", updated_policy)

    # add permisiion to policy
    controller.add_permission(policy_name="ExamplePolicy", permission=permission2)

    # get policy  by name
    policy = controller.get_policy(policy_name="ExamplePolicy")
    print("Retrieved Policy:", policy.to_dict())


    # delete policy
    controller.delete_policy(policy_name="ExamplePolicy")
    print("Policy deleted.")
maim()
