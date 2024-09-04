from typing import List, Dict
from datetime import datetime
import uuid

class UserGroupModel:
    def __init__(self, group_name: str):
        self.group_id = str(uuid.uuid4()) 
        self.name = group_name  
        self.users: List[str] = [] 
        self.policies: List[str] = [] 
        self.create_date = datetime.now()

    def to_dict(self) -> Dict:
        return {
            'group_id': self.group_id,
            'name': self.name,
            'users': self.users,
            'policies': self.policies,
            'create_date': self.create_date
        }
