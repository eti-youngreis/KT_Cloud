# service.py
import hashlib
import uuid
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from DataAccess.userManager import UserNanager
from Models.userModel import User

class AuthenticationService:
    def __init__(self, data_file_path: str):
        self.data_access = UserNanager(data_file_path)
        self.logged_in_users = {}

    def hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def login(self, username: str, password: str) -> str:
        users = self.data_access.load_users()
        user_data = users.get(username)
        if user_data and user_data.get("password") == self.hash_password(password):
            session_id = self.generate_session_id()
            self.logged_in_users[session_id] = username
            return session_id
        else:
            return None

    def logout(self, session_id: str) -> None:
        if session_id in self.logged_in_users:
            self.logged_in_users.pop(session_id)

    def generate_session_id(self):
        return str(uuid.uuid4())

    def is_authenticated(self, session_id: str) -> bool:
        return session_id in self.logged_in_users

    def register(self, username: str, password: str)-> User:
        users = self.data_access.load_users()
        if username in users:
            print(f"User {username} already exists.")
            return False
        hashed_password = self.hash_password(password)
        users[username] = {"password": hashed_password}
        self.data_access.save_users(users)
        print(f"User {username} registered successfully.")
        return True
