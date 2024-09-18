from typing import Dict, Any, Optional
import os
import json
import sqlite3
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DataAccess.ObjectManager import ObjectManager
from DataAccess.StorageManager import StorageManager
from Models.BucketPolicyModel import BucketPolicy

class BucketPolicyManager:

    def __init__(self, policy_path: str = "Bucket_policy.json", db_path:str = "BucketPolicy.db", base_directory: str = "D:/New folder/server"):
        """
        Initializes the BucketPolicyManager with paths for the policy JSON file and the in-memory database.

        Parameters:
        -----------
        policy_path : str, optional
            The path where the bucket policy JSON file is stored (default is "Bucket_policy.json").
        db_path : str, optional
            The path where the SQLite database is stored (default is "BucketPolicy.db").
        base_directory : str, optional
            The base directory where files will be stored (default is "D:/New folder/server").
        """
        self.db_path = os.path.join(base_directory, db_path)
        self.object_manager = ObjectManager(db_path)
        self.object_manager.object_manager.create_management_table(BucketPolicy.object_name, BucketPolicy.table_structure)
        self.policy_path = os.path.join(base_directory, policy_path)
        self.storage_manager = StorageManager(policy_path)
    
    def createInMemoryBucketPolicy(self, bucket_policy: BucketPolicy):
        """
        Stores the bucket policy in the in-memory database.

        Parameters:
        -----------
        bucket_policy : BucketPolicy
            The bucket policy to be saved in-memory.
        """
        self.object_manager.save_in_memory("BucketPolicy", bucket_policy.to_sql())
    
    def createPhysicalPolicy(self, bucket_policy: BucketPolicy) -> bool:
        """
        Saves the bucket policy to a physical JSON file.

        Parameters:
        -----------
        bucket_policy : BucketPolicy
            The bucket policy to be saved to a physical file.

        Returns:
        --------
        bool
            True if the policy was successfully saved, False otherwise.
        """
        if self.storage_manager.is_file_exist(self.policy_path):
            with open(self.policy_path, 'r') as file:
                data = json.load(file)
        else:
            data = {}

        data[bucket_policy.bucket_name] = bucket_policy.to_dict()
        
        with open(self.policy_path, 'w') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        return True
    
    def getBucketPolicy(self, bucket_name: str) -> Optional[Dict]:
        """
        Retrieve a bucket policy by its bucket name from physical storage.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is to be retrieved.

        Returns:
        --------
        Optional[Dict]
            A dictionary representing the bucket policy if found, otherwise None.
        """
        if not self.storage_manager.is_file_exist(self.policy_path):
            return None

        with open(self.policy_path, 'r') as file:
            data = json.load(file)

        return data.get(bucket_name, None)
    
    def deleteInMemoryBucketPolicy(self, bucket_name: str):
        """
        Deletes the bucket policy from the in-memory database.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is to be deleted.
        """
        criteria = f"bucket_name = '{bucket_name}'"
        self.object_manager.delete_from_memory_by_criteria("BucketPolicy", criteria=criteria)
    
    def deletePhysicalPolicy(self, bucket_name: str) -> bool:
        """
        Deletes the bucket policy from the physical JSON file.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is to be deleted.

        Returns:
        --------
        bool
            True if the policy was successfully deleted, False otherwise.
        """
        if not self.storage_manager.is_file_exist(self.policy_path):
            return False

        with open(self.policy_path, 'r') as file:
            data = json.load(file)

        if bucket_name in data:
            del data[bucket_name]
            with open(self.policy_path, 'w') as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
            return True
        
        return False

    def describeBucketPolicy(self, bucket_name: str) -> Optional[Dict]:
        """
        Describes the bucket policy for a specific bucket by retrieving it.

        Parameters:
        -----------
        bucket_name : str
            The name of the bucket whose policy is to be described.

        Returns:
        --------
        Optional[Dict]
            A dictionary representing the bucket policy if found, otherwise None.
        """
        return self.getBucketPolicy(bucket_name)

    def putBucketPolicy(self, bucket_policy: BucketPolicy):
        """
        Updates or applies the bucket policy in both in-memory and physical storage.

        Parameters:
        -----------
        bucket_policy : BucketPolicy
            The bucket policy to be updated or applied.
        """
        if self.storage_manager.is_file_exist(self.policy_path):
            with open(self.policy_path, 'r') as file:
                data = json.load(file)

            # Add or update the policy in the file
            data[bucket_policy['bucket_name']] = bucket_policy

            with open(self.policy_path, 'w') as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
        
        # Update the in-memory database
        permissions = json.dumps(bucket_policy['permissions'])
        updates = f"permissions = '{permissions}'"
        criteria = f"bucket_name = '{bucket_policy['bucket_name']}'"
        self.object_manager.update_in_memory("BucketPolicy", updates=updates, criteria=criteria)
