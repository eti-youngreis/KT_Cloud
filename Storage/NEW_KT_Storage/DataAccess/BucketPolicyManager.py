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
    def __init__(self, path_physical_object: str = "Bucket_policy.json", path_db:str = "BucketPolicy.db", base_directory: str = "D:/New folder/server"):
        '''Initialize ObjectManager with the database connection.'''
        self.path_db = os.path.join(base_directory, path_db)
        self.object_manager = ObjectManager(path_db)
        self.object_manager.object_manager.create_management_table(BucketPolicy.object_name, BucketPolicy.table_structure)
        self.path_physical_object = os.path.join(base_directory, path_physical_object)
        self.storage_manager = StorageManager(path_physical_object)
    
    
    def createInMemoryBucketPolicy(self, bucket_policy):
        self.object_manager.save_in_memory("BucketPolicy", bucket_policy.to_sql())

        
    def createPhysicalObject(self, bucket_policy:BucketPolicy):
        if self.storage_manager.is_file_exist(self.path_physical_object):
            with open(self.path_physical_object, 'r') as file:
                data = json.load(file)
        else:
            data = {}

        data[bucket_policy.bucket_name] = bucket_policy.to_dict()
        
        with open(self.path_physical_object, 'w') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        return True
    
    def get(self, bucket_name: str) -> Optional[Dict]:
        """
        Retrieve a bucket policy by its bucket name.
        :param bucket_name: The name of the bucket.
        :return: The bucket policy as a dictionary, or None if not found.
        """
        if not self.storage_manager.is_file_exist(self.path_physical_object):
            return None

        with open(self.path_physical_object, 'r') as file:
            data = json.load(file)

        return data.get(bucket_name, None)
    
    def deleteInMemoryBucketPolicy(self, bucket_name: str):
        # self.object_manager.delete_from_memory(bucket_name)

        criteria = f"bucket_name = '{bucket_name}'"
        self.object_manager.delete_from_memory_by_criteria("BucketPolicy", criteria=criteria)
        
    def deletePhysicalObject(self, bucket_name: str) -> bool:
        """
        Delete the bucket policy from the physical JSON file.
        :param bucket_name: The name of the bucket to delete.
        :return: True if the policy was deleted, False otherwise.
        """
        if not self.storage_manager.is_file_exist(self.path_physical_object):
            return False

        with open(self.path_physical_object, 'r') as file:
            data = json.load(file)

        if bucket_name in data:
            del data[bucket_name]
            with open(self.path_physical_object, 'w') as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
            return True
        
        return False


    def describeBucketPolicy(self, bucket_name: str) -> Optional[Dict]:
        """
        Describe the bucket policy for a specific bucket.
        :param bucket_name: The name of the bucket.
        :return: The bucket policy as a dictionary, or None if not found.
        """
        return self.get(bucket_name)


    def putBucketPolicy(self, bucket_policy: BucketPolicy):
        # add code to extract all data from self and send it as new updates
        """
        Apply or modify the bucket policy for a specific bucket.
        :param bucket_policy: The BucketPolicyModel object to store.
        """
        with open(self.path_physical_object, 'r') as file:
            data = json.load(file)

        # Add or update the policy in the file
        data[bucket_policy['bucket_name']] = bucket_policy

        with open(self.path_physical_object, 'w') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
            
        permissions = json.dumps(bucket_policy['permissions'])
        updates = f"permissions = '{permissions}'"
        criteria = f"bucket_name = '{bucket_policy['bucket_name']}'"
        self.object_manager.update_in_memory("BucketPolicy", updates=updates, criteria=criteria)
    
    
    # def describe(self, policy_id: str) -> Optional[Dict]:
    #     """
    #     Retrieve a bucket policy by its ID.
    #     :param policy_id: The ID of the bucket policy to retrieve.
    #     :return: The bucket policy as a dictionary, or None if not found.
    #     """
    #     if not self.storage_manager.is_file_exist(self.path_physical_object):
    #         return None

    #     with open(self.path_physical_object, 'r') as file:
    #         data = json.load(file)
        
    #     return data.get(policy_id, None)
    

