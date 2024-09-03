from Services.QuotaService import QuotaService
from typing import Dict, Optional
class QuotaController:
    def __init__(self, service:QuotaService) -> None:
        self.service = service
    
    def create_quota(self, owner_id:str, resource_type:str, limit:int) -> Dict:
        return self.service.create_quota(owner_id, resource_type, limit)

    def delete_quota(self, quota_id:str) -> Dict:
        return self.service.delete_quota(quota_id)
    
    def update_quota(self, quota_id:str, limit:int) -> Dict:
        return self.service.update_quota(quota_id, limit)
    
    def get_quota(self, quota_id:str) -> Dict:
        return self.service.get_quota(quota_id)
    
    def list_quotas(self, owner_id:str) -> Dict:
        return self.service.list_quotas(owner_id)
    
    def check_exceeded(self, quota_id:str) -> bool:
        return self.service.check_exceeded(quota_id)

    def update_usage(self, quota_id:str, amount:int = 1):
        return self.service.update_usage(quota_id, amount)

    def reset_usage(self, quota_id):
        return self.service.reset_usage(quota_id)