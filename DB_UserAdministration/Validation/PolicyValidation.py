from DB_UserAdministration.DataAccess.PolicyManager import PolicyManager


def validate_policy_name(dal: PolicyManager, policy_name: str):
    if not dal.is_identifier_exit(policy_name):
        raise ValueError(f'Policy {policy_name} does not exist')
