import os
from Storage.NEW_KT_Storage.DataAccess.BucketObjectManager import BucketObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.Models.BucketObjectModel import BucketObject
from Storage.NEW_KT_Storage.Service.Abc.STO import STO
import Storage.NEW_KT_Storage.Validation.BucketObjectValiditions as validation
from Storage.NEW_KT_Storage.Controller.VersionController import VersionController
from Storage.NEW_KT_Storage.Controller.LockController import LockController
from Storage.NEW_KT_Storage.Controller.BucketPolicyController import BucketPolicyController


class BucketObjectService(STO):
    def __init__(self, path="C:\\Users\\user1\\Desktop\\server"):
        self.dal = BucketObjectManager(path)
        self.storage_manager = StorageManager(path)
        self.versions = VersionController(path)
        self.locks = LockController(path)
        self.policy = BucketPolicyController(path)


    def validation_for_object(self, bucket_name, object_key):
        if bucket_name is None:
            raise ValueError("bucket_name is required, cant be None")
        if object_key is None:
            raise ValueError("object_key is required, cant be None")
        if not validation.is_bucket_name_valid(bucket_name):
            raise ValueError("Incorrect bucket name")
        if not validation.is_bucket_object_name_valid(object_key):
            raise ValueError("Incorrect object key")
        if not self.storage_manager.is_directory_exist(bucket_name):
            raise ValueError("Bucket not found")
        
        
    def create(self, bucket_name, object_key, content=''):
        '''Create a new Object.'''
        self.validation_for_object(bucket_name, object_key)
        full_path = os.path.join(bucket_name, object_key)
        if self.storage_manager.is_file_exist(full_path):
            raise ValueError("Object already exist")

        # Check for policy perrmission
        if not self.policy.is_action_allowed(bucket_name, "CREATE"):
            raise ValueError(f'The {bucket_name} bucket does not allow CREATE action')
        
        # Create physical object
        self.storage_manager.create_file(full_path, content)

        # Save object in memory
        bucket_object = BucketObject(bucket_name, object_key)
        self.dal.createInMemoryBucketObject(bucket_object)


    def delete(self, bucket_name, object_key, version_id=None):
        '''Delete an existing Object.'''
        self.validation_for_object(bucket_name=bucket_name, object_key=object_key)
        path = os.path.join(bucket_name, object_key)
        if not self.storage_manager.is_file_exist(path):
            raise ValueError("Object not found")
        
        # Check for policy perrmission
        if not self.policy.is_action_allowed(bucket_name, "DELETE"):
            raise ValueError(f'The {bucket_name} bucket does not allow DELETE action')
        
        # Check if object is deletable based on lock
        if self.locks.is_object_deleteable(bucket_name, object_key):
            raise ValueError("Object is locked")
        
        if version_id is None:
            # delete from memory using BucketObjectManager
            self.dal.deleteInMemoryBucketObject(bucket_name, object_key)
            # delete physical object
            self.storage_manager.delete_file(path)
        else:
            self.versions.delete_version_object(bucket_name,object_key,version_id)


    def put(self, bucket_name, object_key, content='', version_id=None):
        '''Modify an existing Object.'''
        self.validation_for_object(bucket_name, object_key)

        if version_id is None:
            # Deletes the old object if it exists
            full_path = os.path.join(bucket_name, object_key)
            # Check for policy perrmission
            if not self.policy.is_action_allowed(bucket_name, "PUT"):
                raise ValueError(f'The {bucket_name} bucket does not allow PUT action')
            # Check if object is updatable based on lock
            if self.locks.is_object_updatable(bucket_name, object_key):
                raise ValueError("Object is locked")
            if self.storage_manager.is_file_exist(full_path):
                self.delete(bucket_name, object_key)
            # Add the object
            self.storage_manager.create_file(full_path, content)
            bucket_object = BucketObject(bucket_name, object_key)
            self.dal.createInMemoryBucketObject(bucket_object)
        else:
            # call the create functions of version
            self.versions.create_version_object(bucket_name, object_key,content)
            

    def get(self, bucket_name, object_key, version_id=None):
        '''get object.'''
        self.validation_for_object(bucket_name=bucket_name, object_key=object_key)
        path = os.path.join(bucket_name, object_key)
        if not self.storage_manager.is_file_exist(path):
            raise ValueError("Object not found")
        
        # check for policy perrmission
        if not self.policy.is_action_allowed(bucket_name, "READ"):
            raise ValueError(f'The {bucket_name} bucket does not allow read action')
        
        if version_id is None:
            path = os.path.join(bucket_name, object_key)
            object_body = self.storage_manager.get_file_content(path)
            object = self.dal.describeBucketObject(bucket_name + object_key)
            object_id, bucket_id, object_key, encryption_id, lock_id, created_at = object[0]
            return BucketObject(bucket_name=bucket_id, object_key=object_key,
                                encryption_id=encryption_id, lock_id=lock_id, content=object_body)
        else:
            # call the function of get version 
            self.versions.get_version_object(bucket_name,object_key,version_id)


    def get_all(self, bucket_name):
        '''get all objects'''
        if bucket_name is None or not validation.is_bucket_name_valid(bucket_name):
            raise ValueError("Incorrect bucket name")
        if not self.storage_manager.is_directory_exist(bucket_name):
            raise ValueError("Bucket not found")
        
        # Check for policy perrmission
        if not self.policy.is_action_allowed(bucket_name, "READ"):
            raise ValueError(f'The {bucket_name} bucket does not allow read action')
        if self.storage_manager.list_files_in_directory(bucket_name) == []:
            raise ValueError("There are no objects in the bucket")
        
        # Retrieve objects from memory 
        objects = self.dal.getAllObjects(bucket_name)
        for index in range(len(objects)):
            object_id, bucket_id, object_key, encryption_id, lock_id, created_at = objects[index]
            path = os.path.join(bucket_name, object_key)
            object_body = self.storage_manager.get_file_content(path)
            objects[index] = BucketObject(bucket_name=bucket_id, object_key=object_key, encryption_id=encryption_id,
                                          lock_id=lock_id, content=object_body)
        return objects
