from typing import Dict
class User:

    def __init__(self, user_name, hash_password, roles = [], policies = [], quotas = None):
        self.user_name = user_name,
        self.hash_password = hash_password
        self.roles = roles,
        self.policies = policies,
        self.quotas = quotas

    def to_dict(self) -> Dict:
        return {
            'user_name': self.user_name,
            'hash_password': self.hash_password,
            'roles': self.roles,
            'policies': self.policies,
            'quotas': self.quotas
        }