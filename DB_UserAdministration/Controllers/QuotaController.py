from DB_UserAdministration.Services.QuotaService import QuotaService
from typing import Dict, Optional
class QuotaController:
    def __init__(self, service:QuotaService) -> None:
        self.service = service
    
    def update_quota(self, owner_id: str, resource_type:str, limit:int) -> Dict:
        return self.service.update_quota(owner_id, resource_type, limit)
    
    def get_quota(self, owner_id: str, resource_type:str) -> Dict:
        return self.service.get_quota(owner_id, resource_type)
    
    def list_quotas(self, owner_id:str) -> Dict:
        return self.service.list_quotas(owner_id)
    
    def check_exceeded(self, owner_id: str, resource_type:str) -> bool:
        return self.service.check_exceeded(owner_id, resource_type)

    def update_usage(self, owner_id: str, resource_type:str, amount:int = 1):
        return self.service.update_usage(owner_id, resource_type, amount)

    def reset_usage(self, owner_id: str, resource_type:str):
        return self.service.reset_usage(owner_id, resource_type)