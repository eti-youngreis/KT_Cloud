import json
import os
# from Models.UserModel import User
# from Models.PermissionModel import Permission
# from Models.PolicyModel import Policy
# from Models.GroupModel import Group
# from Models.QuotaModel import Quota
# from Models.RoleModel import Role

class UserDAL:
    FILE_PATH = 'users.json'

    def __init__(self):
        self.users = self.load_users_from_file()

    def load_users_from_file(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, "r") as file:
                return json.load(file)
        return {}

    def save_users_to_file(self):
        with open(self.FILE_PATH, "w") as file:
            json.dump(self.users, file)

    def get_all_users(self):
        return list(self.users.values())

    def save_user(self, user):
        """Helper method to save a user and persist the data."""
        self.users[user.user_id] = user.__dict__
        self.save_users_to_file()
        return user

    def get_user(self, user_id):
        return self.users.get(user_id)

    def delete_user(self, user_id: str):
        """Delete a user by ID and persist the data."""
        deleted_user = self.users.pop(user_id, None)
        self.save_users_to_file()
        return deleted_user

    def get_user_by_username(self, username: str):
        """Retrieve a user by username."""
        return next(
            (user for user in self.users.values() if user["username"] == username), None
        )

    def update_user(self, user_update):
        """Update an existing user and persist the data."""
        return self.modify_user(user_update)

    def add_to_group(self, user_update):
        """Add a user to a group and persist the data."""
        return self.modify_user(user_update)

    def remove_from_group(self, user_update):
        """Remove a user from a group and persist the data."""
        return self.modify_user(user_update)

    def add_permission(self, user_update):
        """Add a permission to a user and persist the data."""
        return self.modify_user(user_update)

    def remove_permission(self, user_update):
        """Remove a permission from a user and persist the data."""
        return self.modify_user(user_update)

    def set_quota(self, user_update):
        """Set a quota for a user and persist the data."""
        return self.modify_user(user_update)

    def add_policy(self, user_update):
        """Add a policy to a user and persist the data."""
        return self.modify_user(user_update)

    def modify_user(self, user_update):
        """Helper method to modify a user and persist the data."""
        user = self.get_user(user_update.user_id)
        if user:
            self.users[user_update.user_id] = user_update.__dict__
            self.save_users_to_file()
            return user
        return None
