from typing import Dict, Any
import sys
sys.path.append('C:/Users/תמר מליק/bootcamp/project/KT_Cloud/NEW_KT_Storage')
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Models.LockModel import LockModel

class LockManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file= db_file, type = "Lock")
        self.create_table()

    def create_table(self):
            # table_columns = "LockId TEXT PRIMARY KEY", "BucketKey TEXT", "ObjectKey TEXT","RetainUntil" ,"LockMode TEXT", "Unit TEXT"
            table_columns = "object_id TEXT PRIMARY KEY", "BucketKey TEXT", "ObjectKey TEXT","RetainUntil" ,"LockMode TEXT", "Unit TEXT"
            columns_str = ", ".join(table_columns)
            self.object_manager.object_manager.db_manager.create_table("mng_Locks", columns_str)

    def createInMemoryLock(self, lock: LockModel):
        self.object_manager.save_in_memory(lock.to_sql())

    def getInMemoryLock(self, lock_id: str):
        return self.object_manager.get_from_memory(lock_id)

    def deleteInMemoryLock(self, lock: LockModel):
        self.object_manager.delete_from_memory(lock.pk_column, lock.pk_value,lock.lock_id)


    def describeLock(self):
        self.object_manager.get_from_memory()


    def putLock(self):
        # add code to extract all data from self and send it as new updates
        updates = ''

        self.object_manager.update_in_memory(updates)
    
