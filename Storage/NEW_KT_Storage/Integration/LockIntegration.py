from datetime import datetime
import time
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Controller.LockController import LockController

# demonstrate lock functionality
print('''---------------------Start Of session----------------------''')
start_time_session = datetime.now()
print(f"{start_time_session} demonstration of object: Lock start")

start_time = datetime.now()
lock_controller = LockController()

# create lock
print()

bucket_key = "exampleBucket"
object_key = "exampleObj111"
lock_mode = "all"  
amount = 1
unit = "h"  
lock_id = f"{bucket_key}.{object_key}"

print(f"{datetime.now()} start creating lock 'id: {lock_id}'")

lock_controller.create_lock(bucket_key=bucket_key, object_key=object_key, lock_mode=lock_mode, amount=amount, unit=unit)
print(f"{datetime.now()} lock: '{lock_id}' created successfully")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of create lock is: '{total_duration}'")
print()
time.sleep(20)

# get lock
print()
start_time = datetime.now()
print(f"{start_time} start getting lock '{lock_id}'")
lock_example = lock_controller.get_lock(lock_id)
print(f"{datetime.now()} verify lock 'id: {lock_example.lock_id}, "
    f"bucket_key: {lock_example.bucket_key}, object_key: {lock_example.object_key}, "
    f"retain_until = {lock_example.retain_until}, lock_mode: {lock_example.lock_mode}'")

end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of get lock is: '{total_duration}'")
print()
time.sleep(20)

# check if object is updatable
print()
start_time = datetime.now()
print(f"{start_time} start checking if object '{object_key}' in bucket '{bucket_key}' is updatable")
is_updatable = lock_controller.is_object_updatable(bucket_key=bucket_key, object_key=object_key)
print(f"{datetime.now()} object '{object_key}' in bucket '{bucket_key}' is updatable: {is_updatable}")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of updatable check is: '{total_duration}'")
print()
time.sleep(20)

# check if object is deletable
print()
start_time = datetime.now()
print(f"{start_time} start checking if object '{object_key}' in bucket '{bucket_key}' is deletable")
is_deletable = lock_controller.is_object_deletable(bucket_key=bucket_key, object_key=object_key)
print(f"{datetime.now()} object '{object_key}' in bucket '{bucket_key}' is deletable: {is_deletable}")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of deletable check is: '{total_duration}'")
print()
time.sleep(20)

# delete lock
print()
start_time = datetime.now()
print(f"{start_time} start deleting lock '{lock_id}'")
lock_controller.delete_lock(lock_id)
print(f"{datetime.now()} verify lock '{lock_id}' deleted by checking if it exists")
end_time = datetime.now()
total_duration = end_time - start_time
print(f"total duration of delete lock is: '{total_duration}'")

# test deletion of lock by attempting to get it
print()
try:
    lock_controller.get_lock(lock_id)
except Exception as e:
    print(f"Error as expected: {e}")
print()

# end of session
end_time_session = datetime.now()
print(f"{end_time_session} demonstration of object lock ended successfully")
total_duration_session = end_time_session - start_time_session
print(f"the total duration of session lock is: '{total_duration_session}'")
print('''---------------------End Of session----------------------''')
