from QuotaModel import Quota
from typing import Dict
from DataAccess.QuotaManager import QuotaManager
from Exceptions.QuotaException import *

class QuotaService:
    def __init__(self, dal: QuotaManager) -> None:
        """
        Initializes the QuotaService with a Data Access Layer (DAL) for managing quota data.

        Args:
            dal (QuotaManager): An instance of QuotaManager for interacting with quota data storage.
        """
        self.dal: QuotaManager = dal
    
    def create_quota(self, owner_id: str, resource_type: str, limit: int) -> Dict:
        """
        Creates a new quota for a specific owner and resource type.

        Args:
            owner_id (str): The identifier of the owner (user or entity) for whom the quota is being created.
            resource_type (str): The type of resource the quota is associated with (e.g., storage, API requests).
            limit (int): The maximum allowed usage for the quota.

        Returns:
            Dict
        """
        quota_id = owner_id + "_" + resource_type
        quota = Quota(quota_id, resource_type, limit)
        self.dal.create(quota.to_dict())
        return quota.to_dict()

    def delete_quota(self, quota_id: str):
        """
        Deletes an existing quota based on its unique identifier.

        Args:
            quota_id (str): The unique identifier of the quota to be deleted.

        Raises:
            QuotaNotFoundFault: If the quota with the specified ID does not exist.

        Returns:
            None
        """
        if quota_id not in self.dal.get_all_quotas():
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        self.dal.delete(quota_id)
        
    def update_quota(self, quota_id: str, limit: int) -> Dict:
        """
        Updates the limit of an existing quota.

        Args:
            quota_id (str): The unique identifier of the quota to be updated.
            limit (int): The new limit to set for the quota.

        Raises:
            QuotaNotFoundFault: If the quota with the specified ID does not exist.

        Returns:
            None
        """
        if quota_id not in self.dal.get_all_quotas():
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        quota_dict = self.dal.get(quota_id)
        quota_dict['limit'] = limit
        self.dal.update(quota_id, quota_dict)
        return quota_dict

    def get_quota(self, quota_id: str):
        """
        Retrieves details of a specific quota based on its unique identifier.

        Args:
            quota_id (str): The unique identifier of the quota to be retrieved.

        Raises:
            QuotaNotFoundFault: If the quota with the specified ID does not exist.

        Returns:
            dict: A dictionary containing quota details.
        """
        if quota_id not in self.dal.get_all_quotas():
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        return self.dal.get(quota_id)
    
    def list_quotas(self, owner_id: str) -> Dict:
        """
        Lists all quotas associated with a specific user.

        Args:
            user_id (str): The unique identifier of the user whose quotas are to be listed.

        Returns:
            list: A list of quota dictionaries associated with the user.
        """
        res = self.dal.get_quotas_by_owner_id(owner_id)
        if not res:
            raise OwnerNotFoundFault(f'owner with id {owner_id} does not have quotas')  
        return res
      
    def check_exceeded(self, quota_id: str) -> bool:
        """
        Checks if the current usage of a quota has exceeded its limit.

        Args:
            quota_id (str): The unique identifier of the quota to check.

        Returns:
            bool: True if the usage has exceeded the limit, False otherwise.
        """
        pass

    def add_to_usage(self, quota_id: str, amount: int) -> None:
        """
        Increases the current usage of a quota by a specified amount.

        Args:
            quota_id (str): The unique identifier of the quota whose usage is to be increased.
            amount (int): The amount to add to the current usage.

        Returns:
            None
        """
        pass
    
    def sub_from_usage(self, quota_id: str, amount: int) -> None:
        """
        Decreases the current usage of a quota by a specified amount.

        Args:
            quota_id (str): The unique identifier of the quota whose usage is to be decreased.
            amount (int): The amount to subtract from the current usage.

        Returns:
            None
        """
        pass

    def reset_usage(self, quota_id: str) -> None:
        """
        Resets the current usage of a quota to zero.

        Args:
            quota_id (str): The unique identifier of the quota whose usage is to be reset.

        Returns:
            None
        """
        pass
