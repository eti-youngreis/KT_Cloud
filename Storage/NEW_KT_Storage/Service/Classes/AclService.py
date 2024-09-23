from Models.AclModel import Acl
from DataAccess.StorageManager import StorageManager
from Validation import Validation
from DataAccess.AclManager import AclManager


class AclService():
    def __init__(self, dal: AclManager,storage_path ):
        self.dal = dal
        self.storage_manager = StorageManager(storage_path)
        self.acl_list = self.load_acl_objects()

    def load_acl_objects(self):
        return self.dal.object_manager.get_all_objects_from_memory("Acl")
        # return [Acl_objects(bucket_name=row[0], owner=row[1], region=row[2], create_at=row[3]) for row in data_list]

    
    
    # validations here
    

    def create(self,bucket_id, name,user_id,permissions=[] ):
        acl = Acl(bucket_id, name,user_id,permissions)
        created_acl = self.dal.createInMemoryAclObject(acl)
        print(f"ACL Object {created_acl.acl_id} created and saved in memory.")

        return created_acl

    def delete(self,acl_name):
        '''Delete an existing BucketObject.'''
        acl = self.get(acl_name)
        self.buckets.remove(acl)
        self.storage_manager.delete_directory(acl_name)
        delete_result = self.dal.deleteInMemoryAclObject(acl.acl_id)
        return delete_result
        
    def get(self,acl_name:str):
        '''get code object.'''
        # return real time object
        return self.dal.get_acl_object_from_memory(acl_name)
