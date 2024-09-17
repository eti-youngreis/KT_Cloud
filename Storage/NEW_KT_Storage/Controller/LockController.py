from typing import List
import sys
sys.path.append('C:/Users/תמר מליק/bootcamp/project/KT_Cloud')

from Storage.NEW_KT_Storage.Models.LockModel import LockModel
from Storage.NEW_KT_Storage.Service.Classes.LockService import LockService


class LockController:
    def __init__(self, service: LockService):
        self.service = service
    def create_lock(self, bucket_key: str, lock_mode: str, amount: int, unit: str, object_key: str = '*'):

        return self.service.create_lock(bucket_key= bucket_key, object_key=object_key, lock_mode=lock_mode, amount=amount, unit=unit)

    def delete_lock(self, lock_id: str):
        self.service.delete_lock(lock_id)

    def get_lock(self, lock_id: str):
        return self.service.get_lock(lock_id)

    # def delete_lock_from_bucket(self, bucketObject: BucketObject, bucket_name):
    #     self.service.remove_object_from_bucket(bucketObject, bucket_name)

    # def update_object_in_bucket(self,bucketObject_name,updatedObject, bucket_name):
    #     self.service.update_object_in_a_bucket(bucketObject_name,updatedObject, bucket_name)
    
def main():
    lock_service = LockService()
    lock_controller = LockController(service=lock_service)

    # Create a lock
    bucket_key = "test3_bucket"
    object_key = "obj1"
    lock_mode = "all"  # Can be "write", "read", "delete", or "all"
    amount = 1
    unit = "d"  # Lock for 1 day

    # try:
    created_lock = lock_controller.create_lock(bucket_key, lock_mode, amount, unit, object_key)
    print(f"Lock created successfully: {created_lock.lock_id}")

        # Verify the lock was created
    retrieved_lock: LockModel = lock_controller.get_lock(created_lock.lock_id)
    print(f"Retrieved lock: {retrieved_lock.lock_id}")

    # except Exception as e:
    #     print(f"An error occurred: {str(e)}")

main()

