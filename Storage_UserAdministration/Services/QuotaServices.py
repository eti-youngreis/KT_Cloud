from typing import Dict, List, Optional
from DataAccess.QuotaManager import QuotaManager
from Models.QuotaModel import QuotaModel as Quota
# from Storage.Service.Abc import STO
# from Validation import is_valid_bucket_name, is_valid_policy_name

class QuotaService():
    def __init__(self, dal:QuotaManager):
        self.dal = dal
        
    # def __init__(self, user_service, group_service, role_service, dal:QuotaManager):
    #     self.user_service = user_service
    #     self.group_service = group_service
    #     self.role_service = role_service
    #     self.dal = dal

    def create(self, name: str, entity_type: str, resource_type: str, restriction_type: str, limit: int, period: str, usage: int = 0):
        """Create a new Quota."""
        if not name or not entity_type or not resource_type or not restriction_type or not limit or not period or not usage:
            raise ValueError("missing required variables")
        if self.dal.exists(name):
            raise ValueError(f"Quota '{name}' already exists.")
        if entity_type != 'user' and entity_type != 'group' and entity_type != 'role':
            raise ValueError(f"entity '{entity_type}' cannot have quota.")
        quota = Quota(name,  resource_type, restriction_type, limit, period, usage)
        self.dal.insert(quota)
        return quota
    
    def list_quotas(self) -> List[Quota]:
        """List all quotas"""
        quotas = self.dal.select_all('Quota')
        return [Quota(q) for q in quotas]

    def update(self, name: str, **kwargs):
        """Update an existing Quota."""
        #Checking the existence of the Quota
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota '{name}' does not exist.")

        # Dynamic update of the fields sent as parameters
        for key, value in kwargs.items():
            if hasattr(quota, key):
                setattr(quota, key, value)

        self.dal.update(quota)
        return quota
    
    def delete(self, name: str):
        """Delete an existing Quota."""
        # Check if the quota exists
        if not self.dal.exists(name):
            raise ValueError(f"Quota '{name}' does not exist.")
        
        # Perform the deletion
        self.dal.delete(name)
        
    def get(self, name: str) -> Optional[Quota]:
        """Get a Quota by its name."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota '{name}' does not exist.")
        return Quota(**quota)
    
    def reset_usage(self, name: str):
        """Reset the usage of a quota."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota '{name}' does not exist.")
        quota.reset_usage()
        self.dal.update(quota)
        
    def check_exceeded(self, name: str) -> bool:
        """Check if a quota is exceeded."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota '{name}' does not exist.")
        return quota.check_exceeded()