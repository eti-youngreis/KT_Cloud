from typing import Dict, List, Optional
from DataAccess.QuotaManager import QuotaManager
from Models.QuotaModel import QuotaModel as Quota
# # from Abc import STO
# # from Validation import is_valid_bucket_name, is_valid_policy_name

# class QuotaService():
#     def __init__(self, dal:QuotaManager):
#         self.dal = dal
        
#     # def __init__(self, user_service, group_service, role_service, dal:QuotaManager):
#     #     self.user_service = user_service
#     #     self.group_service = group_service
#     #     self.role_service = role_service
#     #     self.dal = dal

#     def create(self, name: str, entity_type: str, resource_type: str, restriction_type: str, limit: int, period: str, usage: int = 0):
#         """Create a new Quota."""
#         if not all([name, entity_type, resource_type, restriction_type, period]) or limit is None or usage is None:
#             raise ValueError("missing required variables")
#         if self.dal.exists(name):
#             raise ValueError(f"Quota {name} already exists.")
#         if entity_type!= 'user' and entity_type!= 'group' and entity_type!= 'role':
#             raise ValueError(f"entity {entity_type} cannot have quota.")
#         quota = Quota(name, entity_type, resource_type, restriction_type, limit, period, usage)
#         self.dal.insert(quota)
#         return quota
    
#     def list_quotas(self) -> List[Quota]:
#         """List all quotas"""
#         quotas = self.dal.select_all('Quota')
#         return [Quota(q) for q in quotas]

#     def update(self, name: str, **kwargs):
#         """Update an existing Quota."""
#         #Checking the existence of the Quota
#         quota = self.dal.select(name)
#         if not quota:
#             raise ValueError(f"Quota {name} does not exist.")

#         # Dynamic update of the fields sent as parameters
#         for key, value in kwargs.items():
#             if hasattr(quota, key):
#                 setattr(quota, key, value)

#         self.dal.update(quota)
#         return quota

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

    def update_usage(self, name, usage_amount):
        if usage_amount<0:
            raise ValueError("The usage amount to increase cannot be negative.")
        quota= self.dal.select(name)
        if not quota:
            raise ValueError(f"Quota {name} does not exist.")
        quota.add_usage(usage_amount)
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
