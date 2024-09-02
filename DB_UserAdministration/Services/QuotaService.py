from QuotaModel import Quota
from DataAccess.QuotaManager import QuotaManager

class QuotaService:
    def __init__(self, dal:QuotaManager) -> None:
        self.dal = dal
        self.quotas = {}
    
    def create_quota(self, user_id, resource_type, limit):
        # This would normally save the quota to a database or similar.
        return Quota(resource_type, limit, self.period)

    def delete_quota(self, quota_id):
        pass
     
    def update_quota(self):
        pass
    
    def get_quota(self):
        pass
    
    def list_quotas(self, user_id):
        pass
    
    def check_exceeded(self):
        pass

    def update_usage(self, amount):
        pass

    def reset_usage(self):
        pass