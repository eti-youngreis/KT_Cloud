from DataAccess import UserManager
from Models import UserModel
from Abc import DBO
from Validations import validation 
import hashlib

class userService(DBO):
    def __init__(self, dal: UserManager):
        self.dal = dal

    def create(self, userName, password, roles = [], policies = [], quotas = None ):
       
        if not self.is_valid_userName(userName):
            raise ValueError("Invalid email address.")
        
        hashed_password = self.hash_password(password)

        user = UserModel.User(userName, hashed_password, roles, policies, quotas)

        # dal
        

    def is_valid_userName(self, userName):
        
        if not validation.is_valid_email(userName):
            return False
        if not self.dal.is_value_exit_in_column("users", "userName", userName):
            return False
        return True

    
    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()