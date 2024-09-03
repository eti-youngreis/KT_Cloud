from typing import Dict, Optional
from DataAccess.QoutaManager import QuotaManager
from Models.QuotaModel import QuotaModel as Quota
from Abc import STO
from Validation import is_valid_bucket_name, is_valid_policy_name

class QuotaService(STO):
    def __init__(self, user_service, group_service, role_service, dal:QuotaManager):
        self.user_service = user_service
        self.group_service = group_service
        self.role_service = role_service
        self.dal = dal

    def create(self, name: str, entity_type: str, resource_type: str, restriction_type: str, limit: int, period: str, usage: int = 0):
        """Create a new Quota."""
        if not name or not entity_type or not resource_type or not restriction_type or not limit or not period or not usage:
            raise ValueError("missing required variables")
        if self.dal.exists(name):
            raise ValueError(f"Quota '{name}' already exists.")
        quota = Quota(name, entity_type, resource_type, restriction_type, limit, period, usage)
        self.dal.insert(quota)
        return quota
