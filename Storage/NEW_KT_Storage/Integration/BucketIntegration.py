from datetime import datetime
import pytest
import os
import sys
os.chdir('D:/s3_project/KT_Cloud')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..')))
from Storage.NEW_KT_Storage.Controller.BucketController import BucketController

# demonstrate all object functionallity
print('''---------------------Start Of session----------------------''')
start_time_session = datetime.now()
print(f"{start_time_session} deonstration of object: Bucket start")

start_time = datetime.now()
bucket_controller = BucketController()

# create
print()
name_bucket = "example-bucket"
owner = "Malki"
region = "us-east-2"
print(f"{datetime.now()} going to create bucket 'name: {name_bucket}, owner: {owner}, region: {region}'")
bucket_controller.create_bucket(name_bucket,owner,region)
print(f"{datetime.now()} bucket name: '{name_bucket}' created successfully")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"the total duration of create bucket is: '{total_duration}'")


# get
print()
start_time = datetime.now()
print(f"{start_time} going to get bucket '{name_bucket}'")
bucket_example = bucket_controller.get_bucket(name_bucket)
print(f"{datetime.now()} verify bucket 'name: {bucket_example.bucket_name}, owner: {bucket_example.owner}, \
region: {bucket_example.region}, created_at: {bucket_example.create_at}'")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"the total duration of get bucket is: '{total_duration}'")


# delete
print()
start_time = datetime.now()
print(f"{start_time} going to delete bucket '{name_bucket}'")
bucket_controller.delete_bucket(name_bucket)
print(f"{datetime.now()} verify bucket '{name_bucket}' deleted by checking if it exist")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"the total duration of delete bucket is: '{total_duration}'")

# test delete bucket
print()
start_time = datetime.now()
pytest.main(['-v', 'Storage/NEW_KT_Storage/Test/BucketTest.py::test_delete_bucket_existing'])
print(f"{datetime.now()} test delete bucket successfully")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"the total duration of test bucket is: '{total_duration}'")

# test create bucket
print()
start_time = datetime.now()
pytest.main(['-v', 'Storage/NEW_KT_Storage/Test/BucketTest.py::test_create_bucket_valid'])
print(f"{datetime.now()} test create bucket successfully")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"the total duration of test bucket is: '{total_duration}'")

print()
end_time_session = datetime.now()
print(f"{end_time_session} deonstration of object Bucket ended successfully")
total_duration_session = end_time_session - start_time_session
print(f"the total duration of session bucket is: '{total_duration_session}'")
print('''---------------------End Of session----------------------''')

