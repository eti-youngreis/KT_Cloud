import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Service.Classes import BucketPolicyService

class BucketPolicyController:
    def __init__(self, service: BucketPolicyService):
        self.service = service


    def create_bucket_policy(self, bucket_policy):
        return self.service.create(bucket_policy)


    def delete_bucket_policy(self, bucket_name):
        self.service.delete(bucket_name)


    # def put_bucket_object(self, updates):
    #     self.service.put(updates)
    

    def modify_bucket_policy(self, bucket_name, permissions):
        self.service.modify(bucket_name, permissions)


    def get_bucket_policy(self, bucket_name):
        return self.service.get(bucket_name)
        
