import sys
import time
sys.path.append('../KT_Cloud')
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
    
    def can_update_object(self, bucket_key: str, object_key: str):
        return self.service.can_update_object(bucket_key, object_key)
    
    def can_delete_object(self, bucket_key: str, object_key: str):
        return self.service.can_delete_object(bucket_key, object_key)
    
def main():
    lock_service = LockService()
    lock_controller = LockController(service=lock_service)

    # Create a lock
    bucket_key = "test3_bucket"
    object_key = "today7bj"
    lock_mode = "all"  
    amount = 1
    unit = "m"  
    
    lock_service.create_lock(bucket_key=bucket_key, object_key=object_key, lock_mode=lock_mode, amount=amount, unit=unit)
    # lock_service.lock_manager.object_manager.object_manager.get_from_memory("Lock", criteria="LockId == 'test3_bucket.obj6'")
    
    # lock_controller.delete_lock("test3_bucket.obj6")
    
    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Main thread interrupted.")

main()

