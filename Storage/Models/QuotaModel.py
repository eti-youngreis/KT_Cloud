class QuotaModel:
    def __init__(self, name: str, resource_type: str, restriction_type: str, limit: int, period: str, usage: int = 0):
        self.name = name
        self.resource_type = resource_type
        self.restriction_type = restriction_type
        self.limit = limit
        self.period = period
        self.usage = usage
        
    def add_usage(self, amount: int):
        self.usage += amount
        if self.usage> self.limit:
            raise Exception(f"Quota exceeded for {self.name}! Resource: {self.resource_type}, Restriction: {self.restriction_type}, Limit: {self.limit}, Usage: {self.usage}")

    def reset_usage(self):
        self.usage = 0
        
    def __str__(self):
            return f"Quota(name={self.name}, resource_type={self.resource_type}, restriction_type={self.restriction_type}, limit={self.limit}, period={self.period}, usage={self.usage})"
    
    def to_dict(self) -> dict:
            return {
                'name': self.name,
                'resource_type': self.resource_type,
                'restriction_type': self.restriction_type,
                'limit': self.limit,
                'period': self.period,
                'usage': self.usage
            }