from Storage.NEW_KT_Storage.Service.Classes.BucketObjectService import BucketObjectService
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..')))

class BucketObjectController:
    def __init__(self):
        self.service = BucketObjectService("C:\\Users\\user1\\Desktop\\server")

    def create_bucket_object(self, bucket_name, object_key,content=''):
        self.service.create(bucket_name, object_key, content)

    def delete_bucket_object(self, bucket_name, object_key, version_id=None):
        self.service.delete(bucket_name, object_key, version_id)

    def put_bucket_object(self,bucket_name, object_key, content='',version_id=None):
        self.service.put(bucket_name, object_key, content, version_id)

    def get_bucket_object(self, bucket_name, object_name, version_id=None):
        return self.service.get(bucket_name, object_name, version_id)

    def list_all_objects(self, bucket_name):
        return self.service.get_all(bucket_name)


