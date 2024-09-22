from datetime import datetime, timedelta
from sortedcontainers import SortedList
import sys
import threading
import time
sys.path.append('../KT_Cloud')
from Storage.NEW_KT_Storage.Models.LockModel import LockModel
from Storage.NEW_KT_Storage.DataAccess.LockManager import LockManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
import Storage.NEW_KT_Storage.Validation.LockValidations as LockValidations


class LockService:
    def __init__(self):
        self.lock_manager = LockManager('D:/s3_project/tables/Locks.db')
        self.storageManager = StorageManager('D:/s3_project/server')
        # Initialize locks from the database
        existing_locks = self.lock_manager.getAllLocks()
        # Initialize locks_IDs_list with existing locks in a sorted list sorted by retain_until
        self.locks_IDs_list = SortedList([(lock.lock_id, lock.retain_until) for lock in existing_locks], key=lambda x: x[1])
        # Initialize lock_map with existing locks-> lock_id : LockModel
        self.lock_map = {lock.lock_id: lock for lock in existing_locks}
        print("Background process: starting lock cleanup scheduler")
        self.start_lock_cleanup_scheduler()


    def create_lock(self, bucket_key: str, object_key: str, lock_mode: str, amount: int, unit: str):
        """Create a new lock for an object."""
        lock_id = f"{bucket_key}.{object_key}"
        
        # Check validations
        LockValidations.validate_lock_not_exist(lock_id, self.lock_map)
        LockValidations.validate_lock_mode(lock_mode)
        LockValidations.validate_time_amount(amount, unit)

        retain_until = self.calculate_retention_duration(amount, unit)
        lock = LockModel(bucket_key, object_key, retain_until, lock_mode)
        
        # Add lockId : retainUntil to sorted list 
        self.locks_IDs_list.add((lock.lock_id, lock.retain_until))
        # Store the lock object itself in the dictionary
        self.lock_map[lock.lock_id] = lock 

        # Physical object - store the lock as a file
        lock_file_path = f"buckets\\{bucket_key}\\locks\\{lock_id}.json"
        self.storageManager.create_file(lock_file_path, lock.to_string())
        # Save in memory
        self.lock_manager.createInMemoryLock(lock)
        
        return lock


    def delete_lock(self, lock_id: str):
        """Delete a lock."""
        LockValidations.validate_lock_exists(lock_id, self.lock_map)
        
        # Retrieve the lock object from dictionary
        lock = self.get_lock(lock_id)
        
        # Remove from sorted list
        lock_tuple = (lock.lock_id, lock.retain_until)
        if lock_tuple in self.locks_IDs_list:
            self.locks_IDs_list.remove(lock_tuple)
        # Remove the lock from the dictionary
        self.lock_map.pop(lock_id)
        # Physical object - delete the lock file
        lock_file_path = f"buckets\\{lock.bucket_key}\\locks\\{lock.lock_id}.json"
        self.storageManager.delete_file(lock_file_path)
        # Remove from in-memory store
        self.lock_manager.deleteInMemoryLock(lock)


    def get_lock(self, lock_id: str):
        """Get a lock by lock_id."""        
        LockValidations.validate_lock_exists(lock_id, self.lock_map)
        return self.lock_map[lock_id]
    

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


    def is_lock_expired(self, lock: LockModel) -> bool:
        """Check if a lock has expired."""
        return lock.retain_until < datetime.now()


    def remove_expired_locks(self):
        """Remove locks that have expired."""        
        now = datetime.now()
        
        expired_locks = []
        
        while self.locks_IDs_list and self.locks_IDs_list[0][1] < now:
            # Remove the expired lock from the list
            expired_lock_id, _ = self.locks_IDs_list.pop(0) 
            # Add the expired lock's ID to the list for deletion
            expired_locks.append(expired_lock_id)  
            
        print(f"(Background) Deleting expired locks: {expired_locks}")
        # Use delete_lock to remove 
        for lock_id in expired_locks:
            self.delete_lock(lock_id)
        
        print(f"Background process for completed successfully with {len(expired_locks)} expired locks removed.")
        
    def start_lock_cleanup_scheduler(self):
        """Start a background thread that checks for expired locks every minute."""        
        def run_cleanup():
            while True:
                self.remove_expired_locks()
                print("(Background) Lock cleanup completed.")
                time.sleep(10)  # Check every minute

        cleanup_thread = threading.Thread(target=run_cleanup)
        cleanup_thread.daemon = True # Set the tread to run in the background
        cleanup_thread.start()


    def is_object_updatable(self, bucket_key: str, object_key: str):
        """Check if an object can be updated."""        
        lock_id = f"{bucket_key}.{object_key}"
        if lock_id in self.lock_map:
            lock: LockModel = self.lock_map[lock_id]
            if lock.lock_mode == "write" or lock.lock_mode == "all":
                return False
        return True


    def is_object_deletable(self, bucket_key: str, object_key: str):
        """Check if an object can be deleted."""        
        lock_id = f"{bucket_key}.{object_key}"
        if lock_id in self.lock_map:
            lock: LockModel = self.lock_map[lock_id]
            if lock.lock_mode == "delete" or lock.lock_mode == "all":
                return False
        return True


    def is_locked(self, bucket_key: str, object_key: str):
        """Check if an object is locked."""        
        lock_id = f"{bucket_key}.{object_key}"
        return lock_id in self.lock_map

