from DB_UserAdministration.DataAccess.UserManager import UserManager
from DB_UserAdministration.Models import UserModel
from DB.Service.Abc.DBO import DBO
from DB_UserAdministration.Validations import validation
import hashlib
import json
from sqlite3 import OperationalError
from DB_UserAdministration.Controllers import PolicyController
from DB_UserAdministration.Controllers import DBUserGroupController
from DB_UserAdministration.Controllers import QuotaController

class userService(DBO):
    def __init__(self, dal: UserManager):
        self.dal = dal

    def create(self, user_name, password, roles = [], policies = [], quotas = {} ):
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
    
    def modify(self, user_id, user_name):
        if not self.is_exist_user_id(user_id):
            raise ValueError("Invalid email address.")
        
        if not self.is_valid_user_name(user_name):
            raise ValueError("Invalid email address.")
        
        try:
            self.dal.update(user_id, user_name)
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

    def describe(self, user_id):
        if not self.is_exist_user_id(user_id):
            raise ValueError("Invalid email address.")   

        try:
            details = self.dal.get(user_id)
            return details
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

    def get_all_users(self, requesting_user_id):
        if not self.can(requesting_user_id, "read", "users"):
                    raise PermissionError("This user has no permission for this action")
        try:
            self.dal.get_all_users()
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}') 
        
    def assign_policy(self, requesting_user_id, target_user_id, policy):
        if not self.can(requesting_user_id, "write", "policy"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            self.dal.update_metadata(target_user_id, "policies", policy, 'append')
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
    
    def revoke_policy(self, requesting_user_id, target_user_id, policy):
        if not self.can(requesting_user_id, "delete", "policy"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            self.dal.update_metadata(target_user_id, "policies", policy, 'delete')
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
    def add_to_group(self, requesting_user_id, target_user_id, group_name):
        if not self.can(requesting_user_id, "write", "group"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            DBUserGroupController.add_member_to_group(group_name, target_user_id)
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
    
    def remove_from_group(self, requesting_user_id, target_user_id, group_name):
        if not self.can(requesting_user_id, "delete", "group"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            DBUserGroupController.remove_member_from_group(group_name, target_user_id)
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
    def get_quotas(self, requesting_user_id, target_user_id):
        #In principle, you need to check appropriate permissions, not possible at the moment
        # if not self.can(requesting_user_id, "delete", "group"):
        #     raise PermissionError("This user has no permission for this action")

        try:
            user_details = self.describe(target_user_id)
            if user_details and user_details['metadata']:
                user_details_dict = json.loads(user_details)
                quotas = user_details_dict.get('quotas')
                return quotas
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
    def add_quota(self, requesting_user_id, target_user_id, quota):
        if not self.can(requesting_user_id, "write", "user"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            self.dal.update_metadata(target_user_id, "quotas", quota, 'update')
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
    def remove_quota(self, requesting_user_id, target_user_id, quota):
        if not self.can(requesting_user_id, "delete", "user"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            self.dal.update_metadata(target_user_id, "quotas", quota, 'delete')
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
    def check_quota(self, requesting_user_id, target_user_id, quota_resource_type, amount):
        if not self.can(requesting_user_id, "read", "user"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            quota = QuotaController.get_quota( target_user_id, quota_resource_type)
            if int(quota.usage) + int(amount) <= int(quota.limit):
                return True
            return False
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
    def add_quota_usage(self, requesting_user_id, target_user_id, resource_type, amount):
        if not self.can(requesting_user_id, "write", "user"):
            raise PermissionError("This user has no permission for this action")
        
        try:
            quota = QuotaController.get_quota( target_user_id, resource_type)
            if int(quota.usage) + int(amount) <= int(quota.limit):
                return True
            return False
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

    def can(self, user_id, action, resource):
        # to check what to do in case of not existing permission in a secific policy...
        user_details = self.describe(user_id)
        user_policy = user_details["policies"]
        for policy in user_policy:
            if not PolicyController.evaluate(policy, action, resource):
                return False
        return True
