
def validate_lifecycle_attributes(policy_name:str, expiration_days:int = None, transitions_days_glacier:int=None, status:str=None):
    if not policy_name or not isinstance(policy_name, str) or len(policy_name) < 3:
        raise ValueError("Policy name must be at least 3 characters long.")
    if expiration_days and not isinstance(expiration_days, int) or expiration_days <= 0:
        raise ValueError("Expiration days must be a positive integer.")
    if transitions_days_glacier and (not isinstance(transitions_days_glacier, int) or transitions_days_glacier < 0):
        raise ValueError("Transitions days to GLACIER must be a non-negative integer.")
    if status not in ["Enabled", "Disabled"]:
        raise ValueError("Status must be either 'Enabled' or 'Disabled'.")
    if expiration_days and transitions_days_glacier and transitions_days_glacier >= expiration_days:
        raise ValueError("Transition days to GLACIER must be less than expiration days.")


def validation_policy_name(policy_name:str):
    if not policy_name or not isinstance(policy_name, str) or len(policy_name) < 3:
        raise ValueError("Policy name must be at least 3 characters long.")


def validate_prefix(prefix):
    if not isinstance(prefix, list):
        raise ValueError("Prefix must be a list.")

    for file_name in prefix:
        if not isinstance(file_name, str):
            raise ValueError(f"'{file_name}' is not a valid string.")
        if '.' not in file_name or file_name.startswith('.') or file_name.endswith('.'):
            raise ValueError(f"'{file_name}' is not a valid file name.")