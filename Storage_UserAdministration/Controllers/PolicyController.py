from typing import List
from Storage_UserAdministration.DataAccess.policyManager import PolicyManager
from Storage_UserAdministration.Models.PolicyModel import PolicyModel
from Storage_UserAdministration.Models.PermissionModel import Permission, Action, Resource, Effect
from Storage_UserAdministration.Services.PolicyService import PolicyService

class PolicyController:
    def __init__(self, service: PolicyService):
        self.service = service

    def create_policy(self, policy_name: str, version: str, permissions: list[int]) -> PolicyModel:
        """Create a new policy."""
        return self.service.create(policy_name, version, permissions)

    def delete_policy(self, policy_name: str) -> str:
        """Delete an existing policy."""
        return self.service.delete(policy_name)

    def update_policy(self, policy_name: str, version: str, permissions: List[Permission]) -> PolicyModel:
        updated_policy = self.service.update(policy_name, version, permissions)
        return updated_policy

    def get_policy(self, policy_name: str) -> PolicyModel:
        """Get a policy by name."""
        return self.service.get(policy_name)

    def list_policies(self) -> List[PolicyModel]:
        """List all policies."""
        return self.service.list_policies()

    def add_permission(self, policy_name: str, permission: Permission):
        """Add a permission to an existing policy."""
        self.service.add_permission(policy_name, permission)

    def evaluate_policy(self, policy_name: str, action: Action, resource: Resource) -> bool:
        """Evaluate if a policy allows the required permissions."""
        return self.service.evaluate(policy_name, action, resource)

def main():
    storage = PolicyManager()
    service = PolicyService(storage)
    controller = PolicyController(service=service)

    # New permission by Enum
    permission1 = Permission.get_id_by_permission(Action.READ, Resource.BUCKET, Effect.ALLOW)
    permission2 = Permission.get_id_by_permission(Action.WRITE, Resource.BUCKET, Effect.DENY)

    # create policy
    new_policy = controller.create_policy(policy_name="ExamplePolicy", version="2024-09-01", permissions=[permission1, permission2])
    print("Created Policy:", new_policy.to_dict())

    # update policy
    updated_policy = controller.update_policy(policy_name="ExamplePolicy", version="2024-09-03", permissions=[permission1])
    print("Updated Policy:", updated_policy)

    # add permission to policy
    controller.add_permission(policy_name="ExamplePolicy", permission=permission2)

    # get policy by name
    policy = controller.get_policy(policy_name="ExamplePolicy")
    print("Retrieved Policy:", policy.to_dict())

    # delete policy
    controller.delete_policy(policy_name="ExamplePolicy")
    print("Policy deleted.")

    # list policy
    list_policies = controller.list_policies()
    print("all policies: ", list_policies)

    # evaluate
    can_read = controller.evaluate_policy(policy_name="ExamplePolicy", action=Action.READ, resource=Resource.BUCKET)
    can_write = controller.evaluate_policy(policy_name="ExamplePolicy", action=Action.WRITE, resource=Resource.BUCKET)
    print(f"Can read: {can_read}")
    print(f"Can write: {can_write}")
main()

