from DB_UserAdministration.DataAccess.UserManager import UserManager
from DB_UserAdministration.Models import UserModel
from DB.Service.Abc.DBO import DBO
from DB_UserAdministration.Validations import validation
import hashlib
from sqlite3 import OperationalError

class userService(DBO):
    def __init__(self, dal: UserManager):
        self.dal = dal

    def create(self, user_name, password, roles = [], policies = [], quotas = None ):
        if not self.is_valid_user_name(user_name):
            raise ValueError("Invalid email address.")
        
        hashed_password = self.hash_password(password)

        user = UserModel.User(user_name, hashed_password, roles, policies, quotas)

        try:
            self.dal.create(user.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}') 
        
    def is_valid_user_name(self, user_name):
        if not validation.is_valid_email(user_name):
            return False
        if not self.dal.is_value_exist_in_column("users", "user_name", user_name):
            return False
        return True

    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()
    
    def delete(self, user_id):
        if not self.is_exist_user_id(user_id):
            raise ValueError("Invalid email address.")
        try:
            self.dal.delete(user_id)
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
    def is_exist_user_id(self, user_id):
        if not self.dal.is_value_exist_in_column("users", "user_id", user_id):
            return False
        return True
    
    def update(self, user_id, user_name):
        if not self.is_exist_user_id(user_id):
            raise ValueError("Invalid email address.")
        
        if not self.is_valid_user_name(user_name):
            raise ValueError("Invalid email address.")
        
        try:
            self.dal.update(user_id, user_name)
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

    def get_user(self, user_id):
        if not self.is_exist_user_id(user_id):
            raise ValueError("Invalid email address.")   

        try:
            details = self.dal.get(user_id)
            return details
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

    def get_all_users(self):

        try:
            self.dal.get_all_users()
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}') 
        
