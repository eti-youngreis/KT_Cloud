from typing import Optional
class Quota:
    def __init__(self, name:str, resource_type:str, owner_id:str, limit:int, period:Optional[str] = None):
        self.name = name
        self.resource_type = resource_type,
        self.owner_id = owner_id 
        self.limit = limit
        self.period = period
        self.usage = 0

    
    def to_dict(self):
        return {
            "name": self.name,
            "resource_type":self.resource_type,
            "owner_id":self.owner_id,
            "limit": self.limit,
            "period": self.period,
            "usage": self.usage
        }
    
    def modify(self, limit):
        self.limit = limit

    def check_exceeded(self):
        return self.usage >= self.limit

    def add_to_usage(self, amount = 1):
        self.usage += amount
    
    def sub_from_usage(self, amount = 1):
        """sub amount from usage"""
        self.usage -= amount

    def reset_usage(self):
        self.usage = 0


