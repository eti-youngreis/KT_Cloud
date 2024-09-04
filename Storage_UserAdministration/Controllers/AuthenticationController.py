import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from Services.AuthenticationService import AuthenticationService

class AuthenticationController:
    def __init__(self):
        self.auth_service = AuthenticationService()

    def login(self, username, password):
        session_id = self.auth_service.login(username, password)
        if session_id:
            print(f"User {username} logged in successfully. Session ID: {session_id}")
            return session_id
        else:
            print("Invalid username or password.")
            raise ValueError("Invalid username or password.")

    def logout(self, username: str):
        try:
            self.auth_service.logout(username)
            print("User logged out successfully.")
        except KeyError:
            raise ValueError("Invalid session ID.")

    def check_authentication(self, username):
        if self.auth_service.is_authenticated(username):
            print("User is authenticated.")
        else:
            print("User is not authenticated.")

    def register(self, username, password):
        if self.auth_service.register(username, password):
            print(f"User {username} registered successfully.")
        else:
            print(f"Failed to register user {username}.")
            raise ValueError(f"Failed to register user {username}.")

# Example usage
if __name__ == "__main__":
    controller = AuthenticationController()
    # # Register a user
    controller.register('user1', 'password123')
    # # Login a user
    session_id = controller.login('user1', 'password123')
    # Check if the user is authenticated
    controller.check_authentication('user1')
    # Logout the user
    controller.logout('user1')
