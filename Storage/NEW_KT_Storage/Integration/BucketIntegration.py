from datetime import datetime
import pytest
import os
import sys
import time
from Storage.NEW_KT_Storage.Exceptions import BucketExceptions
os.chdir('D:/s3_project/KT_Cloud')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..')))
from Storage.NEW_KT_Storage.Controller.BucketController import BucketController

# demonstrate bucket functionality
print('''---------------------Start Of session----------------------''')
start_time_session = datetime.now()
print(f"{start_time_session} demonstration of object: Bucket start")

start_time = datetime.now()
bucket_controller = BucketController()

# create
print()
name_bucket = "example-bucket"
owner = "Malki"
region = "us-east-2"
print(f"{datetime.now()} start creating bucket 'name: {name_bucket}, owner: {owner}, region: {region}'")
bucket_controller.create_bucket(name_bucket,owner, region)
print(f"{datetime.now()} bucket: '{name_bucket}' created successfully")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of create bucket is: '{total_duration}'")
time.sleep(15)

# test create bucket
print()
test_name = 'test_create_bucket_valid'
print(f"Running test: {test_name}:")
pytest.main(['-q', f'Storage/NEW_KT_Storage/Test/BucketTest.py::{test_name}'])


# get
print()
start_time = datetime.now()
print(f"{start_time} start getting bucket '{name_bucket}'")
bucket_example = bucket_controller.get_bucket(name_bucket)
print(f"{datetime.now()} verify bucket 'name: {bucket_example.bucket_name}, owner: {bucket_example.owner}, \
region: {bucket_example.region}, created_at: {bucket_example.create_at}'")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of get bucket is: '{total_duration}'")
time.sleep(5)

# test get bucket
print()
test_name = 'test_get_with_valid_bucket'
print(f"Running test: {test_name}:")
pytest.main(['-q', f'Storage/NEW_KT_Storage/Test/BucketTest.py::{test_name}'])

# delete
print()
start_time = datetime.now()
name_bucket="example-bucket"
print(f"{start_time} start deleting bucket '{name_bucket}'")
bucket_controller.delete_bucket(name_bucket)
print(f"{datetime.now()} verify bucket '{name_bucket}' deleted by checking if it exist")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of delete bucket is: '{total_duration}'")
time.sleep(10)
try:
    bucket_controller.get_bucket(name_bucket)
except BucketExceptions.BucketNotFoundError as e:
    print(e)

# test delete bucket
print()
test_name = 'test_delete_bucket_existing'
print(f"Running test: {test_name}:")
pytest.main(['-q', f'Storage/NEW_KT_Storage/Test/BucketTest.py::{test_name}'])

print()
end_time_session = datetime.now()
print(f"{end_time_session} demonstration of object Bucket ended successfully")
total_duration_session = end_time_session - start_time_session-35
print(f"the total duration of session bucket is: '{total_duration_session}'")
print('''---------------------End Of session----------------------''')

