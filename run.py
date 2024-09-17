import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from KT_Cloud.Storage.NEW_KT_Storage.Controller.BucketPolicyController import BucketPolicyController
from KT_Cloud.Storage.NEW_KT_Storage.Service.Classes.BucketPolicyService import BucketPolicyService
from KT_Cloud.Storage.NEW_KT_Storage.DataAccess.BucketPolicyManager import BucketPolicyManager

P_dal = BucketPolicyManager()
# יצירת אובייקט של השירות
policy_service = BucketPolicyService(P_dal)
# יצירת אובייקט של ה-Controller
policy_controller = BucketPolicyController(policy_service)
# policy = {
#     "policy_id": "123",
#     "bucket_name": "user-uploads",
#     "actions": ["read", "write"],
#     "resources": ["user-uploads/*"],
#     "conditions": {"ip": ["192.168.1.0/24"], "role": "admin"},
#     "version": "2024-09-16"
# }
# policy = {
#     "policy_id":"147",
#     "bucket_name": "my-bucket!",
#     "permissions":{
#             "read", "write", "list"
#         }
# }
# new_policy = policy_controller.create_bucket_policy(policy)
# print("Created Bucket policy:", new_policy)
# policy_controller.create_bucket_policy("your_bucket")
policy_controller.modify_bucket_policy("your_bucket", "delete")
# policy_controller.delete_bucket_policy("your_bucket", "read")
# print(policy_controller.get_bucket_policy("your_bucket"))
# print(policy_controller.get_bucket_policy("user-uploads"))

# update = policy_controller.modify_bucket_policy("user-uploads",{
#             "read": ["user4"],
#             "delete": ["user6"]
#         } )

# delete = policy_controller.delete_bucket_policy("user-uploads")

