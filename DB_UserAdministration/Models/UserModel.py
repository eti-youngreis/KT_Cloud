class User:

    def __init__(self, userName, hash_password, roles = [], policies = [], quotas = None):
        self.userName = userName,
        self.hash_password = hash_password
        self.roles = roles,
        self.policies = policies,
        self.quotas = quotas

