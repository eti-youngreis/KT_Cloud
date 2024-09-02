import hashlib
import re
from KT_Cloud.DB_UserAdministration.Services.Classes import UserService
class User:

    def __init__(self, userName, hash_password, roles = [], policies = [], quotas = None):
        self.userName = userName,
        self.hash_password = hash_password
        self.roles = roles,
        self.policies = policies,
        self.quotas = quotas

