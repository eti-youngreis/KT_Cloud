from typing import Dict, List, Optional
from DataAccess.QuotaManager import QuotaManager
from Models.QuotaModel import QuotaModel as Quota
from typing import Dict, List, Optional
from DataAccess.QuotaManager import QuotaManager
from Models.QuotaModel import QuotaModel as Quota

class QuotaService:
    def __init__(self, dal: QuotaManager):
        self.dal = dal

    def create(self, name: str, resource_type: str, restriction_type: str, limit: int, period: str, usage: int = 0,
            users: Optional[List[str]] = None, groups: Optional[List[str]] = None, roles: Optional[List[str]] = None) -> Quota:
        """Create a new Quota."""
        if not all([name, resource_type, restriction_type, period]) or limit is None:
            raise ValueError("missing required variables")
        if self.dal.exists(name):
            raise ValueError(f"Quota {name} already exists.")
        quota = Quota(name, resource_type, restriction_type, limit, period, usage, users, groups, roles)
        self.dal.insert(quota)
        return quota
    
    def list_quotas(self) -> List[Quota]:
        """List all quotas."""
        return self.dal.list_all().values()
        
    def update(self, name: str, **kwargs):
        """Update an existing Quota."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota {name} does not exist.")

        for key, value in kwargs.items():
            # Dynamic update of the fields sent as parameters
            if hasattr(quota, key):
                setattr(quota, key, value)

        return self.dal.update(quota)

    def update_usage(self, user_quota, amount_usage):
        if amount_usage<0:
            raise ValueError("The usage amount to increase cannot be negative.")
        q_name =user_quota.key()
        quota= self.dal.select(q_name)
        if not quota:
            raise ValueError(f"Quota {q_name} does not exist.")
        
        curr_usage = user_quota.value()

        if quota.check_exceeded(curr_usage,amount_usage):
            return 
        return self.dal.update(quota)

    
    def add_entity(self, name: str, entity_type: str, entity_id: str):
        """Add an entity to the quota."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota {name} does not exist.")
        
        quota.add_entity(entity_type, entity_id)
        self.dal.update(quota)
        return quota

    def remove_entity(self, name: str, entity_type: str, entity_id: str):
        """Remove an entity from the quota."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota {name} does not exist.")
        
        quota.remove_entity(entity_type, entity_id)
        self.dal.update(quota)
        return quota
    
    def get(self, name: str) -> Optional[Quota]:
        """Get a Quota by its name."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota '{name}' does not exist.")
        return quota
    
    def reset_usage(self, name: str):
        """Reset the usage of a quota."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota '{name}' does not exist.")
        quota.reset_usage()
        self.dal.update(quota)
