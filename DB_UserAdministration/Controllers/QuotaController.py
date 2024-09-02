from Services.QuotaService import QuotaService

class QuotaController:
    def __init__(self, service:QuotaService) -> None:
        self.service = service
    
    def create_quota(self, owner_id:str, resource_type:str, limit:int):
        return self.service.create_quota(owner_id, resource_type, limit)

    def delete_quota(self, quota_id):
        return self.service.delete_quota(quota_id)
    
    def update_quota(self, quota_id:str, limit:int):
        return self.service.update_quota(quota_id, limit)
    
    def get_quota(self, quota_id:str):
        return self.service.get_quota(quota_id)
    
    def list_quotas(self, user_id):
        pass
    
    def check_exceeded(self):
        pass

    def update_usage(self, amount):
        pass

    def reset_usage(self):
        pass