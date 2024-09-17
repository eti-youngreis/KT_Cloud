from datetime import datetime, timedelta
import json
import sys
import threading
sys.path.append('C:/Users/תמר מליק/bootcamp/project/KT_Cloud')
from Storage.NEW_KT_Storage.Models.LockModel import LockModel
from Storage.NEW_KT_Storage.DataAccess.LockManager import LockManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
import time


class LockService:
    def __init__(self):
        self.lock_manager = LockManager('D:/s3_project/tables/Locks.db')
        self.storageManager = StorageManager()
        self.locks = {}
        self.start_lock_cleanup_scheduler()
    

    def create_lock(self, bucket_key: str, object_key: str, lock_mode: str, amount: int, unit: str):
        """Create a new lock for an object."""
        print(bucket_key, object_key, lock_mode, amount, unit)
        retain_until = self.calculate_retention_duration(amount, unit)
        lock_id = f"{bucket_key}.{object_key}"
        # RT code object
        if lock_id in self.locks or f"{lock_id}.*" in self.locks:
            raise ValueError(f"{bucket_key}.{object_key} is already locked")
        lock = LockModel(bucket_key, object_key, retain_until, lock_mode)
        self.locks[lock.lock_id] = lock
        # physical object 
        self.storageManager.create_file(f"buckets\\{lock.bucket_key}\\locks\\{lock.lock_id}.json",  lock.to_string())
        
        # save in memory
        self.lock_manager.createInMemoryLock(lock)
    
        return lock
    
    
    # האם את כל הגישות לנעילות לעשות על פי מזהה נעילה או לפי מזהה אובייקט
    # תשובה- מזהה נעילה
    # def get_lock(self, object_key: str):
    #     """Get a lock for an object."""
    #     if object_key not in self.locks:
    #         raise ValueError("Object is not locked")
    #     return self.locks[object_key]

    def delete_lock(self, lock_id: str):
        """Delete a lock."""
        if lock_id not in self.locks:
            raise ValueError("lock is not exist")
        lock = self.locks[lock_id]
        # RT code object
        self.locks.pop(lock_id)
        # physical object
        #lock_file_path = f"locks/{lock.bucket_key}/{lock.object_key}.lock"
        self.storageManager.delete_file(lock)
        # save in memory
        self.lock_manager.deleteInMemoryLock(lock)
    
    def get_lock(self, lock_id: str):
        """Get a lock by name"""
        if lock_id not in self.locks:
            raise ValueError("lock is not exist")
        return self.locks[lock_id]
    
    def calculate_retention_duration(self, amount: int, unit: str):
        """Calculate retention duration based on amount and unit."""
        now = datetime.now()
        if unit == "y":
            return now + timedelta(days=amount * 365)
        elif unit == "d":
            return now + timedelta(days=amount)
        elif unit == "h":
            return now + timedelta(hours=amount)
        elif unit == "m":
            return now + timedelta(minutes=amount)
        else:
            raise ValueError("Unsupported time unit")
        
    
    def is_lock_expired(self, lock : LockModel) -> bool:
        return lock.retain_until < datetime.now()
    
    def remove_expired_locks(self):
        for lock in self.locks.values():
            if self.is_lock_expired(lock):
                self.delete_lock(lock.lock_id)
    
    def start_lock_cleanup_scheduler(self):
        def run_cleanup():
            while True:
                self.remove_expired_locks()
                time.sleep(60) # Check every minute
        cleanup_thread = threading.Thread(target=run_cleanup)
        cleanup_thread.daemon = True
        cleanup_thread.start()

            
    def can_update_object(self, bucket_key: str, object_key: str):
        """Check if an object can be updated."""
        
        lock_name = f"{bucket_key}/{object_key}"
        if lock_name in self.locks:
            lock:LockModel = self.locks[object_key]
            if lock.lock_mode == "write" or lock.lock_mode == "all":
                return False
        return True
    
    def can_delete_object(self, object_key: str):
        """Check if an object can be deleted."""
        if object_key in self.locks:
            lock: LockModel = self.locks[object_key]
            if lock.lock_mode == "delete" or lock.lock_mode == "all":
                return False
        return True
    
    def is_locked(self, bucket_key: str, object_key: str):
        """Check if an object is locked."""
        lock_name = f"{bucket_key}/{object_key}"
        return lock_name in self.locks
    
    
    def release_lock(self, object_key: str):
        """Release the lock for an object."""
        if object_key in self.locks:
            del self.locks[object_key]
