class ACL:
    def __init__(self, owner):
        self.owner = owner
        self.permissions = []

    def add_permission(self, permission):
        self.permissions.append(permission)

    def remove_permission(self, user, permission):
       # Remove user permission
        if user in self.permissions:
            self.permissions[user].discard(permission)
            if not self.permissions[user]:
                del self.permissions[user]

    def check_permission(self, user, permission):
        # Check if the user has a certain permission
        return permission in self.permissions.get(user, set())

    def is_owner(self, user):
        # Check if the user owns the file
        return self.owner == user

    def __str__(self):
        return f"Owner: {self.owner}, Permissions: {self.permissions}"
