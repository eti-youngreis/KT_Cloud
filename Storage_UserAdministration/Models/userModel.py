class User:
    def __init__(self, user_name, hash_password, roles = [], policies = [], quotas = None):
        self.user_name = user_name,
        self.hash_password = hash_password
        self.roles = roles,
        self.policies = policies,
        self.quotas = quotas
