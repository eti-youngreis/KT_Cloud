import hashlib
import uuid
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DataAccess.UserDAL import UserDAL
from Models.userModel import User
class AuthenticationService:
    def __init__(self):
        self.data_access =UserDAL()

    def hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def login(self, username, password):
        user:User = self.data_access.get_user(username)
        if user and user.password == self.hash_password(password):
            session_id = self.generate_session_id()
            user.logged_in=True
            user.token=session_id
            self.data_access.save_user(user)
            return session_id
        else:
            return None

    def logout(self, username):
        user:User = self.data_access.get_user(username)
        user.logged_in=False
        user.token=None
        self.data_access.save_user(user)


    def generate_session_id(self):
        return str(uuid.uuid4())

    def is_authenticated(self, username):
        user:User = self.data_access.get_user(username)
        return user.logged_in==True

    def register(self, username, password):
        users = self.data_access.users
        if username in users:
            raise KeyError("duplicate user")
        hashed_password = self.hash_password(password)
        new_user =User(username=username,password=hashed_password)
        self.data_access.save_user(new_user)
        return new_user
