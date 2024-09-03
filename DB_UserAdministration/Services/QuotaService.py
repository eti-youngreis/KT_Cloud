from sqlite3 import OperationalError
from DB_UserAdministration.Models.QuotaModel import Quota
from typing import Dict
from DB_UserAdministration.DataAccess.QuotaManager import QuotaManager
from DB_UserAdministration.Exceptions.QuotaException import *

class QuotaService:
    def __init__(self, dal: QuotaManager) -> None:
        """
        Initializes the QuotaService with a Data Access Layer (DAL) for managing quota data.

        Args:
            dal (QuotaManager): An instance of QuotaManager for interacting with quota data storage.
        """
        self.dal: QuotaManager = dal
        self.quotas:Dict[str: Quota] = {}
        self.limits = {
            'DB_INSTANCE':40,
            'BUCKET':100,
            'OBJECT':300,
            'SNAPSHOT':50,
        }
    
    def _hash_quota_id(self, owner_id:str, resource_type:str):
        return owner_id + "_" + resource_type
    
    def create_default_quotas(self, owner_id:str):
        for type in self.limits.keys():
            self.create_quota(owner_id, type, self.limits[type])
        
    def create_quota(self, owner_id: str, resource_type: str, limit: int) -> Dict:
        """
        Creates a new quota for a specific owner and resource type.

        Args:
            owner_id (str): The identifier of the owner (user or entity) for whom the quota is being created.
            resource_type (str): The type of resource the quota is associated with (e.g., storage, API requests).
            limit (int): The maximum allowed usage for the quota.

        Returns:
            dict: A dictionary containing new quota details.
        """
        quota_id = self._hash_quota_id(owner_id, resource_type)
        if quota_id in self.quotas:
            raise QuotaAlreadyExistFault(f'quota with owner id {owner_id} and resource type {resource_type} already exist')
        quota = Quota(quota_id, resource_type, owner_id, limit)
        self.quotas[quota_id] = quota
        try:
            self.dal.create(quota_id, quota.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
        return {quota_id: quota.to_dict()}

    def delete_quota(self, owner_id: str, resource_type:str) -> Dict:
        """
        Deletes an existing quota based on its unique identifier.

        Args:
            quota_id (str): The unique identifier of the quota to be deleted.

        Raises:
            QuotaNotFoundFault: If the quota with the specified ID does not exist.

        Returns:
            dict: A dictionary containing deleted quota details.
        """
        quota_id = self._hash_quota_id(owner_id, resource_type)
        if quota_id not in self.quotas:
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        quota_description = self.quotas[quota_id].to_dict()
        del self.quotas[quota_id]
        try:
            self.dal.delete(quota_id)
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        return {quota_id: quota_description}
        
        
    def update_quota(self, owner_id: str, resource_type:str, limit: int) -> Dict:
        """
        Updates the limit of an existing quota.

        Args:
            quota_id (str): The unique identifier of the quota to be updated.
            limit (int): The new limit to set for the quota.

        Raises:
            QuotaNotFoundFault: If the quota with the specified ID does not exist.

        Returns:
            dict: A dictionary containing updated quota details.
        """
        quota_id = self._hash_quota_id(owner_id, resource_type)
        if quota_id not in self.quotas:
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        quota:Quota = self.quotas[quota_id]
        quota.modify(limit)
        try:
            self.dal.update(quota_id, quota.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
        return {quota_id: quota.to_dict()}

    def get_quota(self, owner_id: str, resource_type:str):
        """
        Retrieves details of a specific quota based on its unique identifier.

        Args:
            quota_id (str): The unique identifier of the quota to be retrieved.

        Raises:
            QuotaNotFoundFault: If the quota with the specified ID does not exist.

        Returns:
            dict: A dictionary containing quota details.
        """
        quota_id = self._hash_quota_id(owner_id, resource_type)
        if quota_id not in self.quotas:
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        return {quota_id: self.quotas[quota_id].to_dict()}
    
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
      
    def check_exceeded(self, owner_id: str, resource_type:str) -> bool:
        """
        Checks if the current usage of a quota has exceeded its limit.

        Args:
            quota_id (str): The unique identifier of the quota to check.

        Raises:
            QuotaNotFoundFault: If the quota with the specified quota_id is not found.

        Returns:
            bool: True if the usage has exceeded the limit, False otherwise.
        """
        quota_id = self._hash_quota_id(owner_id, resource_type)
        if quota_id not in self.quotas:
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        quota: Quota = self.quotas[quota_id]
        return quota.check_exceeded()

        

    def update_usage(self, owner_id: str, resource_type:str, amount: int = 1) -> None:
        """
        Updates the current usage of a quota by adding or subtracting a specified amount.

        Args:
            quota_id (str): The unique identifier of the quota whose usage is to be updated.
            amount (int): The amount to add to or subtract from the current usage. A positive value increases usage,
                        while a negative value decreases it.

        Raises:
            QuotaNotFoundFault: If the quota with the specified quota_id is not found.
            ValueError: If an internal error occurs while updating the quota.

        Returns:
            None
        """
        quota_id = self._hash_quota_id(owner_id, resource_type)
        if quota_id not in self.quotas:
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        quota: Quota = self.quotas[quota_id]
        if amount > 0:
            quota.add_to_usage(amount)
        elif amount < 0:
            quota.sub_from_usage(amount)
        try:
            self.dal.update(quota_id, quota.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')

        
        


    def reset_usage(self, owner_id: str, resource_type:str) -> None:
        """
        Resets the current usage of a quota to zero.

        Args:
            quota_id (str): The unique identifier of the quota whose usage is to be reset.

        Returns:
            None
        """
        quota_id = self._hash_quota_id(owner_id, resource_type)
        if quota_id not in self.quotas:
            raise QuotaNotFoundFault(f'Quota with id {quota_id} not found')
        quota:Quota = self.quotas[quota_id]
        quota.reset_usage()
        try:
            self.dal.update(quota_id, quota.to_dict())
        except OperationalError as e:
            raise ValueError(f'An internal error occurred: {str(e)}')
        
        
