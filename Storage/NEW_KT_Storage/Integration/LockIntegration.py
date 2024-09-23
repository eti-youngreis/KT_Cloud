from datetime import datetime
import time
import os
import sys
from Storage.NEW_KT_Storage.Integration.MainIntegration import print_colored_line
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))
from Storage.NEW_KT_Storage.Controller.LockController import LockController


print_colored_line('''---------------------Start Of session----------------------''')
start_time_session = datetime.now()
print_colored_line(f"{start_time_session} demonstration of object: Lock start", "yellow")

start_time = datetime.now()
lock_controller = LockController()

# create lock
print()

bucket_key = "myBucket"
object_key = "myLastObj"
lock_mode = "write"  
amount = 10
unit = "d"  
lock_id = f"{bucket_key}.{object_key}"

print_colored_line(f"{datetime.now()} start creating lock 'id: {lock_id}'", "yellow")

lock_controller.create_lock(bucket_key=bucket_key, object_key=object_key, lock_mode=lock_mode, amount=amount, unit=unit)
print_colored_line(f"{datetime.now()} lock: '{lock_id}' created successfully", "green")
end_time = datetime.now()
total_duration = end_time - start_time
print_colored_line(f"total duration of create lock is: '{total_duration}'", "yellow")
print()
time.sleep(20)

# get lock
print()
start_time = datetime.now()
print_colored_line(f"{start_time} start getting lock '{lock_id}'", "yellow")
lock_example = lock_controller.get_lock(lock_id)
print_colored_line(f"{datetime.now()} verify lock 'id: {lock_example.lock_id}, "
    f"bucket_key: {lock_example.bucket_key}, object_key: {lock_example.object_key}, "
    f"retain_until = {lock_example.retain_until}, lock_mode: {lock_example.lock_mode}'", "green")

end_time = datetime.now()
total_duration = end_time - start_time
print_colored_line(f"total duration of get lock is: '{total_duration}'", "yellow")
print()
time.sleep(20)

# check if object is updatable
print()
start_time = datetime.now()
print_colored_line(f"{start_time} start checking if object '{object_key}' in bucket '{bucket_key}' is updatable", "yellow")
is_updatable = lock_controller.is_object_updatable(bucket_key=bucket_key, object_key=object_key)
print_colored_line(f"{datetime.now()} object '{object_key}' in bucket '{bucket_key}' is updatable: {is_updatable}", "green")
end_time = datetime.now()
total_duration = end_time - start_time
print_colored_line(f"total duration of updatable check is: '{total_duration}'", "yellow")
print()
time.sleep(20)

# check if object is deletable
print()
start_time = datetime.now()
print_colored_line(f"{start_time} start checking if object '{object_key}' in bucket '{bucket_key}' is deletable", "yellow")
is_deletable = lock_controller.is_object_deletable(bucket_key=bucket_key, object_key=object_key)
print_colored_line(f"{datetime.now()} object '{object_key}' in bucket '{bucket_key}' is deletable: {is_deletable}", "green")
end_time = datetime.now()
total_duration = end_time - start_time
print_colored_line(f"total duration of deletable check is: '{total_duration}'", "yellow")
print()
time.sleep(20)

# delete lock
print()
start_time = datetime.now()
print_colored_line(f"{start_time} start deleting lock '{lock_id}'", "yellow")
lock_controller.delete_lock(lock_id)
print_colored_line(f"{datetime.now()} lock: '{lock_id}' deleted successfully", "green")

end_time = datetime.now()
total_duration = end_time - start_time
print_colored_line(f"total duration of delete lock is: '{total_duration}'", "yellow")

# test deletion of lock by attempting to get it
print()
print_colored_line(f"{datetime.now()} verify lock '{lock_id}' was deleted by trying to get it", "reset")
try:
    lock_controller.get_lock(lock_id)
except Exception as e:
    print_colored_line(f"Error as expected: {e}", "red")
print()

# end of session
end_time_session = datetime.now()
print_colored_line(f"{end_time_session} demonstration of object lock ended successfully", "yellow")
total_duration_session = end_time_session - start_time_session
print_colored_line(f"the total duration of session lock is: '{total_duration_session}'", "yellow")
print('''---------------------End Of session----------------------''')
