import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Service.Classes.BucketPolicyService import BucketPolicyService
from Models.BucketPolicyModel import BucketPolicyActions

class BucketPolicyController:
    def __init__(self, service = BucketPolicyService()):
        self.service = service


    def create_bucket_policy(self, bucket_name = None, actions = None, allow_versions = False):

        return self.service.create(bucket_name, actions, allow_versions)


    def delete_bucket_policy(self, bucket_name):
        self.service.delete(bucket_name)


    # def put_bucket_object(self, updates):
    #     self.service.put(updates)
    

    def modify_bucket_policy(self, bucket_name, update_actions=[],allow_versions=None, action=None):
        self.service.modify(bucket_name = bucket_name, update_actions = update_actions, allow_versions = allow_versions, action = action)


    def get_bucket_policy(self, bucket_name):
        return self.service.get(bucket_name)
    
    def is_action_allowed(self, bucket_name, action_name):
        return self.service.is_action_allowed(bucket_name, action_name)
    
    def is_versions_allowed(self, bucket_name):
        return self.service.is_versions_allowed(bucket_name)
    

        
