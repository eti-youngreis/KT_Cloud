from datetime import datetime
from typing import Dict, Any
import sys
sys.path.append('../NEW_KT_Storage')
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.LockModel import LockModel

class LockManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file=db_file)
        self.object_name = "Lock"
        self.object_manager.object_manager.db_manager.create_table("mng_Locks", LockModel.table_structure)

    def createInMemoryLock(self, lock: LockModel):
        self.object_manager.save_in_memory(self.object_name, lock.to_sql())

    def getInMemoryLock(self, lock_id: str):
        return self.object_manager.get_from_memory(lock_id)
    
    def getAllLocks(self):
        lock_records = self.object_manager.get_all_objects_from_memory(self.object_name)
        if not lock_records:
            return []
        return [LockModel(bucket_key, object_key, datetime.strptime(retain_until, '%Y-%m-%d %H:%M:%S.%f'), lock_mode) 
                for lock_id, bucket_key, object_key, retain_until, lock_mode in lock_records]

    def deleteInMemoryLock(self, lock: LockModel):
        self.object_manager.delete_from_memory_by_pk(self.object_name, lock.pk_column, lock.pk_value)


    def describeLock(self):
        self.object_manager.get_from_memory()


    def putLock(self):
        # add code to extract all data from self and send it as new updates
        updates = ''

        self.object_manager.update_in_memory(updates)
    
