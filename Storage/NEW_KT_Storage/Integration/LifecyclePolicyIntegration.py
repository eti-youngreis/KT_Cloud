import os
import sys
import time
from datetime import datetime
from Storage.NEW_KT_Storage.Controller.LifecyclePolicyController import LifecyclePolicyController
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy
from Storage.NEW_KT_Storage.Controller.BucketController import BucketController
os.chdir('C:\\Users\\User\\Desktop\\לימודים תשפד\\BootCamp\\KT_Cloud')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..','..')))
def get_current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


bucket_controller = BucketController()
lifecycle_controller = LifecyclePolicyController()

print(f"---------------------Start Of Session----------------------")
start_time = datetime.now()
print(f"{get_current_time()} Demonstration of LifecyclePolicyController functionality starts")
print(start_time)


print("\n---------------------Create Bucket----------------------")
name_bucket = "demoBucket"
owner = "ASuccessfulCompany"
region = "us-east-1"
print(f"{get_current_time()} start creating bucket 'name: {name_bucket}, owner: {owner}, region: {region}'")
bucket_controller.create_bucket(name_bucket, owner, region)
print(f"{get_current_time()} bucket: '{name_bucket}' created successfully")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of create bucket is: '{total_duration}'")
time.sleep(5)

print("\n---------------------Get Bucket----------------------")
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

print("\n---------------------Create LifecyclePolicy----------------------")
# Sample policy
lifecycle_name = "lifecycle"
bucket_name = "demoBucket"
print(f"{get_current_time()} start creating lifecycle policy 'name: {lifecycle_name}', bucket_name: '{bucket_name}'")
start_creation_date_time = datetime.now()
lifecycle_controller.create("lifecycle", "bucket_name", 70, 30, "Enabled", ["object", "documents"])
end_creation_date_time = datetime.now()
print(f"Total creation time: {end_creation_date_time - start_creation_date_time}")
time.sleep(5)

print("\n---------------------Get LifecyclePolicy----------------------")
start_get_date_time = datetime.now()
print(f"{get_current_time()} Now the object will be taken from the DB")
lifecycle_policy = lifecycle_controller.get(lifecycle_name)
print(f"\nThis function was executed successfully")
end_get_date_time = datetime.now()
print(f"Total get lifecycle time: {end_get_date_time - start_get_date_time}")
time.sleep(5)

print("\n---------------------Describe LifecyclePolicy----------------------")
start_description_date_time = datetime.now()
print(f"{start_description_date_time} going to describe LifecyclePolicy ")
description = lifecycle_controller.describe(policy_name=lifecycle_name)
print(f"\nLifecycle Policy description: {description}\n")
end_description_date_time = datetime.now()
print(f"{end_description_date_time} LifecyclePolicy 'lifecycle' described successfully")
print(f"Total description time: {end_description_date_time - start_description_date_time}")
time.sleep(5)

print("\n---------------------Modify LifecyclePolicy----------------------")
start_modification_date_time = datetime.now()
retrieved_policy = lifecycle_controller.get(lifecycle_policy.policy_name)
print(f"{get_current_time()}  going to modify db LifecyclePolicy ")
lifecycle_controller.modify(policy_name=retrieved_policy.policy_name, expiration_days=100)
end_modification_date_time = datetime.now()
print(f"{end_modification_date_time} LifecyclePolicy modified successfully")
print(f"Total modification time: {end_modification_date_time - start_modification_date_time}")
time.sleep(5)

print("\n---------------------simulate LifecyclePolicy----------------------")
start_simulate_date_time = datetime.now()
print(f"{get_current_time()} going to simulate LifecyclePolicy")
lifecycle_controller.simulate_and_visualize_policy_impact(
    policy_name="lifecycle",
    object_count=10000,
    avg_size_mb=100,
    simulation_days=100
)
end_simulate_date_time = datetime.now()
print(f"{get_current_time()} delete 'lifecycle' successfully")
print(f"Total simulate time: {end_simulate_date_time - start_simulate_date_time}")
time.sleep(5)

print("\n---------------------Delete Bucket----------------------")
start_delete_date_time = datetime.now()
print(f"{get_current_time()} going to delete Bucket 'demoBucket'")
deleted =bucket_controller.delete_bucket("demoBucket")
end_delete_date_time = datetime.now()
print(f"{get_current_time()} delete 'demoBucket' successfully")
print(f"Total deletion time: {end_delete_date_time - start_delete_date_time}")
time.sleep(5)


print("\n---------------------Delete LifecyclePolicy----------------------")
start_delete_date_time = datetime.now()
print(f"{get_current_time()} going to delete lifecycle 'lifecycle'")
deleted_lifecycle =lifecycle_controller.delete("lifecycle")
end_delete_date_time = datetime.now()
print(f"{get_current_time()} delete 'lifecycle' successfully")
print(f"Total deletion time: {end_delete_date_time - start_delete_date_time}")
time.sleep(5)


