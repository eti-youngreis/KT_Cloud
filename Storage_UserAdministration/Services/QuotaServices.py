from typing import Dict, List, Optional
from DataAccess.QuotaManager import QuotaManager
from Models.QuotaModel import QuotaModel as Quota
# from Storage.Service.Abc import STO
# from Validation import is_valid_bucket_name, is_valid_policy_name

class QuotaService():
    def __init__(self, dal:QuotaManager):
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
        return self.dal.list_all()
        
    def update(self, name: str, **kwargs):
        """Update an existing Quota."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota {name} does not exist.")

        for key, value in kwargs.items():
            # Dynamic update of the fields sent as parameters
            if hasattr(quota, key):
                setattr(quota, key, value)

        self.dal.update(quota)
        return quota

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
    
    def delete(self, name: str):
        """Delete an existing Quota."""
        # Check if the quota exists
        if not self.dal.exists(name):
            raise ValueError(f"Quota '{name}' does not exist.")
        quota = self.dal.select(name)
        for user in quota.users:
            self.dal.delete(user, "users", quota.name)
        for group in quota.groups:
            self.dal.delete(group, "groups")
        for role in quota.roles:
            self.dal.delete(role, "roles")
        # Perform the deletion
        self.dal.delete_quota(name)
        
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
        
    def check_exceeded(self, name: str) -> bool:
        """Check if a quota is exceeded."""
        quota = self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota '{name}' does not exist.")
        return quota.check_exceeded()