from typing import Dict, Any
import json
import sqlite3
from DataAccess import ObjectManager

class BucketPolicyManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.create_table()

    
    def createInMemoryBucketPolicy(self):
        self.object_manager.save_in_memory()


    def deleteInMemoryBucketPolicy(self):
        self.object_manager.delete_from_memory()


    def describeBucketPolicy(self):
        self.object_manager.get_from_memory()


    def putBucketPolicy(self):
        # add code to extract all data from self and send it as new updates
        updates = ''

        self.object_manager.update_in_memory(updates)
    
