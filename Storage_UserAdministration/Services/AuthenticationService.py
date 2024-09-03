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
        users = self.data_access.users
        user_data = users.get(username)
        if user_data and user_data.get("password") == self.hash_password(password):
            session_id = self.generate_session_id()
            user_data['logged_in']=True
            user_data['token']=session_id
            self.data_access.save_users_to_file()
            return session_id
        else:
            return None

    def logout(self, username):
        users = self.data_access.users
        user_data = users.get(username)
        user_data['logged_in']=False
        user_data['token']=None
        self.data_access.save_users_to_file()


    def generate_session_id(self):
        return str(uuid.uuid4())

    def is_authenticated(self, username):
        users = self.data_access.users
        user_data = users.get(username)
        return user_data['logged_in']==True

    def register(self, username, password):
        users = self.data_access.users
        if username in users:
            return False
        hashed_password = self.hash_password(password)
        new_user =User(username=username,password=hashed_password)
        users[username] = {'password': hashed_password,'logged_in':False,'token':None}
        self.data_access.save_users_to_file()
        return new_user
