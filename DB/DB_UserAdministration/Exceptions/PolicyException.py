class DuplicatePolicyError(Exception):
    def __init__(self, policy_name):
        super().__init__(f"Policy '{policy_name}' already exists.")
