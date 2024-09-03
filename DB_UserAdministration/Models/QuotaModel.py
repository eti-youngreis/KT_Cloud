from typing import Optional
class Quota:
    def __init__(self, id:str, resource_type:str, owner_id:str, limit:int, period:Optional[str] = None):
        self.id:str = id
        self.resource_type:str = resource_type,
        self.owner_id:str = owner_id 
        self.limit:int = limit
        self.period = period
        self.usage:int = 0

    
    def to_dict(self):
        return {
            "quota_id": self.id,
            "resource_type":"".join(self.resource_type),
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


