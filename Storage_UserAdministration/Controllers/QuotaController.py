from typing import List, Optional
from Models.QuotaModel import QuotaModel as Quota
from Services.QuotaService import QuotaService

class QuotaController:
    def __init__(self, service: QuotaService):
        self.service = service

    def create_quota(self, name: str, resource_type: str, restriction_type: str, limit: int, period: str, usage: int = 0,
                    users: Optional[List[str]] = None, groups: Optional[List[str]] = None, roles: Optional[List[str]] = None) -> Quota:
        """Create a new quota with optional lists for users, groups, and roles."""
        return self.service.create(name, resource_type, restriction_type, limit, period, usage, users, groups, roles)

    def list_quotas(self) -> List[Quota]:
        """List all quotas."""
        return self.service.list_quotas()
    
    def update_quota(self, quota_name: str, entity_type: str, resource_type: str, restriction_type: str, limit: int, period: str, usage: int) -> Quota:
        """Update an existing quota."""
        return self.service.update(
            name=quota_name,
            entity_type=entity_type,
            resource_type=resource_type,
            restriction_type=restriction_type,
            limit=limit,
            period=period,
            usage=usage
        )
    def update_usage(self, quota_name:str, usage: int) -> Quota:
            """Update an existing quota."""
            return self.service.update_usage(quota_name,usage)


