from Services.QuotaService import QuotaService

class QuotaController:
    def __init__(self, service:QuotaService) -> None:
        self.service = service
    
    def create_quota(self, user_id, resource_type, limit):
        return self.service.create_quota(user_id, resource_type, limit)

    def delete_quota(self, quota_id):
        return self.service.delete_quota(quota_id)
    
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