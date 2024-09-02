import json

class UserNanager:
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path

    def load_users(self):
        try:
            with open(self.json_file_path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            print(f"File {self.json_file_path} not found.")
            return {}
        except json.JSONDecodeError:
            print("Error decoding JSON.")
            return {}

    def save_users(self, users):
        with open(self.json_file_path, 'w') as file:
            json.dump(users, file, indent=4)
