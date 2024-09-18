from typing import Dict, Any
import json
import sqlite3
from KT_Cloud.Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager

class BucketObjectManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.create_table()


    def createInMemoryBucketObject(self):
        self.object_manager.save_in_memory()


    def deleteInMemoryBucketObject(self):
        self.object_manager.delete_from_memory()


    def describeBucketObject(self):
        self.object_manager.get_from_memory()


    def putBucketObject(self):
        # add code to extract all data from self and send it as new updates
        updates = ''

        self.object_manager.update_in_memory(updates)
    
