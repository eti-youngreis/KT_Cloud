import json
import os
from userModel import User
class Group:
    def __init__(self, group_name:str, file_path:str='groups.json'):
        self.name:str = group_name
        self.users:User = [] 
        self.roles = []  
        self.permissions = []  
        self.file_path = file_path
        self.load_group()

    def load_data(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as file:
                return json.load(file)
        return {}

    def save_data(self, data):
        with open(self.file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def load_group(self):
        data = self.load_data()
        if self.name in data:
            group_data = data[self.name]
            self.users = group_data.get('users', [])
            self.roles = group_data.get('roles', [])
            self.permissions = group_data.get('permissions', [])
        else:
            data[self.name] = {
                "users": self.users,
                "roles": self.roles,
                "permissions": self.permissions
            }
            self.save_data(data)

    def create_group(self):
        data = self.load_data()
        if self.name in data:
            raise ValueError("this groupvalready exist")
        data[self.name] = {
            "users": self.users,
            "roles": self.roles,
            "permissions": self.permissions
        }
        self.save_data(data)

    def delete_group(self):
        data = self.load_data()
        if self.name in data:
            del data[self.name]
            self.save_data(data)
        else:
            raise ValueError("הקבוצה לא נמצאה")

    def update_group(self, new_group_name):
        data = self.load_data()
        if self.name in data:
            data[new_group_name] = data.pop(self.name)
            data[new_group_name]['name'] = new_group_name
            self.name = new_group_name
            self.save_data(data)
        else:
            raise ValueError("הקבוצה לא נמצאה")

    def get_group(self):
        data = self.load_data()
        if self.name in data:
            return data[self.name]
        else:
            raise ValueError("הקבוצה לא נמצאה")

    def list_groups(self):
        data = self.load_data()
        return list(data.keys())

    def add_member(self, user_id):
        if user_id not in self.users:
            self.users.append(user_id)
            self.save_group()

    def remove_member(self, user_id):
        if user_id in self.users:
            self.users.remove(user_id)
            self.save_group()

    def assign_role(self, role_name):
        if role_name not in self.roles:
            self.roles.append(role_name)
            self.save_group()

    def revoke_role(self, role_name):
        if role_name in self.roles:
            self.roles.remove(role_name)
            self.save_group()

    def assign_permission(self, permission_name):
        if permission_name not in self.permissions:
            self.permissions.append(permission_name)
            self.save_group()

    def revoke_permission(self, permission_name):
        if permission_name in self.permissions:
            self.permissions.remove(permission_name)
            self.save_group()

    def save_group(self):
        data = self.load_data()
        data[self.name] = {
            "users": self.users,
            "roles": self.roles,
            "permissions": self.permissions
        }
        self.save_data(data)


# שימוש לדוגמה:
group = Group("Developers")
group.create_group()

# הוספת משתמש לקבוצה
group.add_member("user123")

# הקצאת תפקיד לקבוצה
group.assign_role("Admin")

# רשימת כל הקבוצות
print(group.list_groups())

# קבלת פרטי קבוצה
print(group.get_group())

# מחיקת קבוצה
group.delete_group()
