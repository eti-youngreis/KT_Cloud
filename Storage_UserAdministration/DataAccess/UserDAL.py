import json
import os
from typing import List, Optional, Dict
from KT_Cloud.Storage_UserAdministration.Models.userModel import User
from KT_Cloud.Storage_UserAdministration.Models.PermissionModel import Permission

class UserDAL:
    def __init__(self, file_path = "C://Users//User//Downloads//users.json"):
        self.file_path = file_path
        self.users: Dict[str, User] = {}
        self.load_users_from_file()

    def load_users_from_file(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as file:
                users_data = json.load(file)
                for username, user_info in users_data["server"]["users"].items():
                    user = User(
                        username=user_info["username"],
                        password=user_info["password"],
                        user_id=user_info["user_id"],
                        email=user_info.get("email"),
                        logged_in=user_info.get("logged_in", False),
                        token=user_info.get("token"),
                        role=user_info.get("role"),
                        policies=user_info.get("policies"),
                        quotas=user_info.get("quotas"),
                        groups=user_info.get("groups"),
                    )
                    self.users[username] = user

    def save_users_to_file(self):
        with open(self.file_path, 'r') as file:
            data = json.load(file)

        users_data = {
            username: user.__dict__ for username, user in self.users.items()
        }
        data['server']['users'] = users_data
        with open(self.file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Users data updated successfully in {self.file_path}.")
  
    def get_all_users(self) -> Dict[str, User]:
        return self.users

    def save_user(self, user: User) -> User:
        self.users[user.username] = user
        self.save_users_to_file()
        return user

    def get_user(self, username: str) -> Optional[User]:
        return self.users.get(username)

    def delete_user(self, username: str) -> Optional[User]:
        deleted_user = self.users.pop(username, None)
        self.save_users_to_file()
        return deleted_user

    def get_user_by_username(self, username: str) -> Optional[User]:
        return next(
            (user for user in self.users.values() if user.username == username), None
        )

    def update_user(self, user_update: User) -> Optional[User]:
        """Modify a user's attributes and persist changes."""
        user = self.get_user(user_update.username)
        if user:
            # Update the user's attributes
            user.username = user_update.username
            user.password = user_update.password
            user.email = user_update.email
            user.logged_in = user_update.logged_in
            user.token = user_update.token
            # Add or update any other attributes like policies, quota, groups, etc.
            self.users[user_update.username] = user
            self.save_users_to_file()
            return user
        return None

    def add_to_group(self, username: str, group: str) -> Optional[User]:
        """Add a group to the user's groups."""
        user = self.get_user(username)
        if user:
            if not hasattr(user, "groups"):
                user.groups = []
            if group not in user.groups:
                user.groups.append(group)
            self.save_users_to_file()
            return user
        return None

    def remove_from_group(self, username: str, group: str) -> Optional[User]:
        """Remove a group from the user's groups."""
        user = self.get_user(username)
        if user and hasattr(user, "groups") and group in user.groups:
            user.groups.remove(group)
            self.save_users_to_file()
            return user
        return None

    def add_permission(self, username: str, permission: Dict[str,int]) -> Optional[User]:
        """Add a permission to the user."""
        user = self.get_user(username)
        if user:
            if not hasattr(user, "permissions"):
                user.permissions = []
            if permission not in user.permissions:
                user.permissions.append(permission)
            self.save_users_to_file()
            return user
        return None

    def remove_permission(self, username: str, permission: Permission) -> Optional[User]:
        """Remove a permission from the user."""
        user = self.get_user(username)
        if user and hasattr(user, "permissions") and permission in user.permissions:
            user.permissions.remove(permission)
            self.save_users_to_file()
            return user
        return None

    def set_quota(self, username: str, quota_name: str) -> Optional[User]:
        """Set the quota for the user."""
        user = self.get_user(username)
        if user:
            self.users[user.username] = user
            self.save_users_to_file()
            return user
        return None

    def add_policy(self, username: str, policy: str) -> Optional[User]:
        """Add a policy to the user."""
        user = self.get_user(username)
        if user:
            self.users[user.username] = user
            self.save_users_to_file()
            return user
        return None