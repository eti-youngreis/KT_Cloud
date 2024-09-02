from typing import List, Dict
from datetime import datetime
import uuid

class UserGroupModel:
    def __init__(self, group_name: str):
        self.group_id = str(uuid.uuid4())  # מזהה ייחודי לקבוצה
        self.name = group_name  # שם הקבוצה
        self.users: List[str] = []  # רשימת המשתמשים בקבוצה
        # self.roles: List[str] = []  # תפקידים המוקצים למשתמשים בקבוצה
        self.policies: List[str] = []  # הרשאות המוקצות לקבוצה
        self.create_date = datetime.now()  # תאריך יצירת הקבוצה

    def to_dict(self) -> Dict:
        return {
            'group_id': self.group_id,
            'name': self.name,
            'users': self.users,
            # 'roles': self.roles,
            'policies': self.policies,
            'create_date': self.create_date
        }
