
# demonstrate all object functionallity
from datetime import datetime
import sys
import os

import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
import time

from Storage.NEW_KT_Storage.Controller.BucketPolicyController import BucketPolicyController
from Storage.NEW_KT_Storage.Service.Classes.BucketPolicyService import BucketPolicyService, IsExistactionFault
from Storage.NEW_KT_Storage.DataAccess.BucketPolicyManager import BucketPolicyManager

bucketPolicy_manager = BucketPolicyManager()
bucketPolicy_service = BucketPolicyService(bucketPolicy_manager)
bucketPolicy_controller = BucketPolicyController(bucketPolicy_service)

def log(message):
    current_time = datetime.now()
    print(f'{current_time}', {message})
    return current_time


print('''---------------------Start Of session----------------------''')
start_session_time = log("demonstration of object: Bucket Policy start")

# create
print()
start_time = datetime.now()
bucket_name = "Efrat_bucket"
start_time = log(f'going to create bucket policy names {bucket_name}')
bucketPolicy_controller.create_bucket_policy(bucket_name)
try:
    bucketPolicy_controller.get_bucket_policy(bucket_name)
except KeyError as e:
    log(f'error {e}')
end_time = log(f'bucket policy to {bucket_name} created successfully')
total_duration = end_time-start_time
log(f"total duration of create bucket is: '{total_duration}'")
time.sleep(10)

# test create bucket policy
test_name = 'test_create_bucket_policy_success'
log(f"Running test: {test_name}:")
pytest.main(['-q', f'D:/boto3 project/KT_Cloud/Storage/NEW_KT_Storage/Test/test_BucketPolicyTests.py::{test_name}'])

# get
print()
start_time = log(f"start getting bucket policy'{bucket_name}'")
bucket_policy = bucketPolicy_controller.get_bucket_policy(bucket_name)
end_time = log(f"verify bucket policy 'name: {bucket_policy['bucket_name']}, actions: {bucket_policy['actions']}, \
allow versions: {bucket_policy['allow_versions']}")
total_duration = end_time - start_time
log(f"total duration of get bucket policy is: '{total_duration}'")
time.sleep(5)

# test get bucket policy
test_name = 'test_get_not_exist_bucket_policy'
log(f"Running test: {test_name}:")
pytest.main(['-q', f'D:/boto3 project/KT_Cloud/Storage/NEW_KT_Storage/Test/test_BucketPolicyTests.py::{test_name}'])

# modify delete actions
print()
start_time = log(f"start modify bucket policy:'{bucket_name}'")
bucket_policy = bucketPolicy_controller.modify_bucket_policy(bucket_name, update_actions=['READ', 'WRITE'], action="delete")
end_time = log(f"bucket policy {bucket_name} modify successfully")
total_duration = end_time - start_time
log(f"total duration of modify bucket policy is: '{total_duration}'")
time.sleep(10)

# test delete action from the bucket policy
test_name = 'test_delete_action_from_bucket_policy'
log(f"Running test: {test_name}:")
pytest.main(['-q', f'D:/boto3 project/KT_Cloud/Storage/NEW_KT_Storage/Test/test_BucketPolicyTests.py::{test_name}'])

# modify add actions
print()
start_time = log(f"start modify bucket policy:'{bucket_name}'")
bucket_policy = bucketPolicy_controller.modify_bucket_policy(bucket_name, update_actions=['READ'], action="add")
end_time = log(f"bucket policy {bucket_name} modify successfully")
total_duration = end_time - start_time
log(f"total duration of modify bucket policy is: '{total_duration}'")
time.sleep(10)

# test get bucket policy
test_name = 'test_add_exist_action'
log(f"Running test: {test_name}:")
pytest.main(['-q', f'D:/boto3 project/KT_Cloud/Storage/NEW_KT_Storage/Test/test_BucketPolicyTests.py::{test_name}'])

# modify allow_versions actions
print()
start_time = log(f"start modify bucket policy:'{bucket_name}'")
bucket_policy = bucketPolicy_controller.modify_bucket_policy(bucket_name, allow_versions=False)
end_time = log(f"bucket policy {bucket_name} modify successfully")
total_duration = end_time - start_time
log(f"total duration of modify bucket policy is: '{total_duration}'")
time.sleep(10)

# test get bucket policy
test_name = 'test_modify_when_allow_versions_invalid'
log(f"Running test: {test_name}:")
pytest.main(['-q', f'D:/boto3 project/KT_Cloud/Storage/NEW_KT_Storage/Test/test_BucketPolicyTests.py::{test_name}'])

# delete
print()
start_time = log(f'going to delete bucket {bucket_name}''')
bucketPolicy_controller.delete_bucket_policy(bucket_name)   
end_time = log(f"bucket policy {bucket_name} deleted successfully")
total_duration = end_time - start_time
log(f"total duration of modify bucket policy is: '{total_duration}'")

# test delete bucket policy
test_name = 'test_delete_exist_policy'
log(f"Running test: {test_name}:")
pytest.main(['-q', f'D:/boto3 project/KT_Cloud/Storage/NEW_KT_Storage/Test/test_BucketPolicyTests.py::{test_name}'])

print()
end_session_time = datetime.now()
log(f"{end_session_time} demonstration of object Bucket Policy ended successfully")
total_duration_session = end_session_time - start_session_time
print(f"the total duration of session bucket is: '{total_duration_session}'")
print('''---------------------End Of session----------------------''')
