import sys
import time
sys.path.append('../KT_Cloud')
from Storage.NEW_KT_Storage.Service.Classes.LockService import LockService


class LockController:
    def __init__(self):
        self.service = LockService()
        
    def create_lock(self, bucket_key: str, lock_mode: str, amount: int, unit: str, object_key: str = '*'):
        return self.service.create_lock(bucket_key= bucket_key, object_key=object_key, lock_mode=lock_mode, amount=amount, unit=unit)


    def delete_lock(self, lock_id: str):
        self.service.delete_lock(lock_id)

    def get_lock(self, lock_id: str):
        return self.service.get_lock(lock_id)
    
    def is_object_updatable(self, bucket_key: str, object_key: str):
        return self.service.is_object_updatable(bucket_key, object_key)
    
    def is_object_deletable(self, bucket_key: str, object_key: str):
        return self.service.is_object_deletable(bucket_key, object_key)
    

